import multiprocessing
import os
import re
import time

import Levenshtein
from lxml import etree

from file_utils import readJson, writeJson
from mp_crawler import releases, release_vers
from conf import conf


def parseCmdHtmls(manual_dir, res_dir):
    """
    Parse MP commands' html files to obtain
        1) Cmd's name and summary;
        2) Cmd's synopsis, i.e., usage templates;
        3) Cmd's options/parameters/operands/... and the corresponding descriptions;
        4) Cmd's examples.
    """
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    cmdhtml_cmdjson_dict, cmdhtml_originalhtml_dict, parsed_cmds = {}, {}, 0

    for release in releases:
        _dir = manual_dir + '/' + release
        for i in range(1, 10):
            if i != 1 and i != 8:
                continue  # only consider the commands in sections 1 and 8
            man = 'man' + str(i)
            man_html_dir = _dir + '/' + man + '-html'
            man_json_dir = res_dir + '/' + release + '/' + man + '-json'
            man_json = _dir + '/' + man + '.json'
            man_htmls = readJson(man_json)['Command links']
            if not os.path.exists(man_json_dir):
                os.makedirs(man_json_dir)
            for name in os.listdir(man_html_dir):
                cmdhtml, cmd_id = man_html_dir + '/' + name, name[:-5]
                cmdjson = man_json_dir + '/' + cmd_id + '.json'
                if not os.path.exists(cmdjson):
                    cmdhtml_cmdjson_dict[cmdhtml] = cmdjson
                    cmdhtml_originalhtml_dict[cmdhtml] = man_htmls[int(cmd_id)]
                else:
                    parsed_cmds += 1
            del man_htmls

    print('# parsed cmds:', parsed_cmds)
    print('# cmds to be parsed:', len(cmdhtml_cmdjson_dict))

    n = len(cmdhtml_cmdjson_dict)
    if n == 0:
        return
    processnum = n if n < 20 else 20
    pool = multiprocessing.Pool(processes=processnum)
    sl = sorted(cmdhtml_cmdjson_dict.items(), key=lambda x: x[0])
    for item in sl:
        pool.apply_async(parseCmdHtml, (item[0], cmdhtml_originalhtml_dict[item[0]], item[1]))
    pool.close()
    pool.join()


def parseCmdHtml(cmdhtml, ori_cmdhtml, cmdjson):
    """
    Parse a command(cmd) html file to extract the cmd names, templates, options/parameters, and examples.
    """
    print('\n' + cmdhtml, '->', cmdjson)
    html = open(cmdhtml, 'r', encoding='utf-8').read()
    e = etree.HTML(html)
    h4_nodes, infoitem_cont_dict = e.xpath("//div[@id='tableWrapper']/h4"), {}
    if h4_nodes is not None and len(h4_nodes) > 0:
        for h4_node in h4_nodes:
            pre_nodes = h4_node.xpath(".//following-sibling::pre")
            if pre_nodes is not None and len(pre_nodes) > 0:
                infoitem = ' '.join(h4_node.xpath(".//b//text()")).lower()
                text = pre_nodes[0].xpath("string(.)")  # type 'str'
                text = text.replace('⎪', '|').replace('—', '-')  # 12923.html, 4s-cluster-start.html
                text = re.sub(' -{2,} ', ' - ', text)  # e.g., 2437.html
                infoitem_cont_dict[infoitem] = {
                    'text': text, 'btext': str(etree.tostring(pre_nodes[0], encoding='utf-8'))  # type 'str(bytes)'
                }
    del e

    name_summ_dict, name_templates_dict, name_descs = {}, {}, []
    names, described_paras, relcmds_dict = set(), {}, {}
    name_lsnum, synopsis_lsnum, standard_lsnums = 0, 0, set()  # two important numbers of left spaces in a cmd html page
    possible_name = extractCmdNameInCmdLink(ori_cmdhtml) if ori_cmdhtml != '' else ''

    if 'name' not in infoitem_cont_dict and 'nom' not in infoitem_cont_dict:  # 12474.html
        possible_name = extractCmdNameInCmdLink(ori_cmdhtml) if ori_cmdhtml != '' else ''
        if possible_name != '':
            name_summ_dict[possible_name] = ''
            names.add(possible_name.lower())

    for infoitem in infoitem_cont_dict:

        text = infoitem_cont_dict[infoitem]['text']
        btext = infoitem_cont_dict[infoitem]['btext']

        if infoitem in {'name', 'nom'}:  # man8-5383.html
            name_summ_dict, name_lsnum = extractCmdNames(text)
            standard_lsnums.add(name_lsnum)
            if len(name_summ_dict) > 0:
                for name in name_summ_dict:
                    # print(name, '->', name_summ_dict[name])
                    names.add(name.lower())  # e.g., 'GAP' in 10015.html
            else:  # Try to find cmd name - summary in 'description' when no cmd name is found in 'name', e.g., 195.html
                if possible_name != '' and 'description' in infoitem_cont_dict:
                    desc_text = infoitem_cont_dict['description']['text']
                    desc_text = re.sub(' +', ' ', desc_text)
                    for part in desc_text.split('\n\n'):
                        part = part.strip('\n ')
                        s, low_name = part.lower(), possible_name.lower()
                        if s.startswith(low_name + ' - ') or s.startswith(low_name + '- '):
                            name_summ_dict[possible_name] = part[len(possible_name)+2:].strip()

        elif infoitem in {'synopsis', 'synapse'}:  # 10395.html
            template_name_dict, synopsis_lsnum = extractTemplatesInSynopsis(text, names, name_lsnum)
            standard_lsnums.add(synopsis_lsnum)
            for template in template_name_dict:
                name = template_name_dict[template]
                if template.lower().startswith(name):
                    name = template[:len(name)]  # '0fffff' -> '0FFFFFF' in 5.html
                # print('\ntemplate:', template, '->', name)
                if name not in name_templates_dict:
                    name_templates_dict[name] = []
                name_templates_dict[name].append(template)
                names.add(name.lower())

        elif infoitem == 'see also':
            relcmds_dict = extractRelatedCmds(btext)

        elif 'author' in infoitem or 'copyright' in infoitem or 'license' in infoitem or 'history' in infoitem \
                or 'exit' in infoitem or 'return' in infoitem:
            pass

        else:
            if len(standard_lsnums) == 0:
                standard_lsnums.add(7)
            sl = sorted(standard_lsnums)
            label_parts_dict = divideText(text, sl)  # 'Operation mode' in tar
            for label in label_parts_dict:
                parts = label_parts_dict[label]
                para_desc_dict = extractParas(parts, btext, names)
                label = infoitem + ' -> ' + label if label != '' else infoitem
                for para in para_desc_dict:
                    desc = para_desc_dict[para]
                    if desc != '':
                        if label not in described_paras:
                            described_paras[label] = []
                        described_paras[label].append({ 'paras': para, 'description': desc })

                for part in parts:  # extract some descriptions about cmd names
                    part = re.sub(' +', ' ', part.strip('\n '))
                    part_words = part.lower().split()
                    if len(part_words) > 20 and re.match('[a-zA-Z0-9]', part_words[0]):
                        if len(names.intersection(set(part_words))) > 0:
                            name_descs.append(part)

    # Try to find cmd templates in 'description' when no template is found in 'synopsis', e.g., 1047.html
    if len(name_templates_dict) == 0 and 'description' in infoitem_cont_dict:
        if possible_name != '':
            names.add(possible_name.lower())
        if len(names) > 0:
            desc_text = infoitem_cont_dict['description']['text']
            desc_text = re.sub(' +', ' ', desc_text)
            for part in desc_text.split('\n\n'):
                part = ' ' + part.strip('\n ') + ' '
                for name in names:
                    l = ' ' + name + ' ['
                    if l in part:
                        s = part[part.find(l):].strip()
                        if s.count('[') == s.count(']'):
                            if name not in name_templates_dict:
                                name_templates_dict[name] = []
                            name_templates_dict[name].append(s)

    # Try to separate cmd templates and their possible descriptions, e.g., 10395.html, 30434.html, man8-1750.html
    revised_templates_dict = {}
    for name in name_templates_dict:
        revised_templates_dict[name] = []
        for template in name_templates_dict[name]:
            new_template, desc = template, ''
            if re.match('.*[\]>] ?#[a-zA-Z-_, ]+\.?$', template):
                sa = template.split('#')
                if len(sa) == 2:
                    new_template, desc = sa[0].strip(), sa[1].strip()
            elif re.match('.*[\]>] ?[A-Z][a-z-_, ]+\.?$', template):
                sa = re.split('[\]>]', template)
                desc = sa[-1].strip()
                new_template = template.replace(desc, '').strip()
            revised_templates_dict[name].append({'template': new_template, 'description': desc})

    if cmdjson != '':
        cmd_dict = {
            'cmd summary': name_summ_dict, 'cmd templates': revised_templates_dict, 'cmd description': name_descs,
            'described paras': described_paras, 'related cmds': relcmds_dict,
        }
        writeJson(cmd_dict, cmdjson)

    del infoitem_cont_dict


def extractCmdNameInCmdLink(link):
    """
    Extract a cmd name from the original cmd link.
    """
    link = link[:-5]
    name = link[link.rfind('/')+1:link.rfind('.')]
    if re.match(r'.*-[0-9]+(.[0-9]+)*$', name):
        name = '-'.join(name.split('-')[:-1])
    return name


def extractCmdNames(text):
    """
    Extract cmd names and their summaries from a <name> text.
    """
    s = text.strip('\n ').replace('\n', ' ')  # 12766.html
    if re.match('[^ ]+$', s) and len(s) < 20:
        return {s: ''}, countLeftSpace(text)

    name_summary_dict, lsnum = {}, 0
    lines, i, _names = text.split('\n'), 0, set()
    while i < len(lines):
        line = lines[i].strip('\n ')
        if '- ' in line:
            ind, _names = line.find('- '), set()
            s, summ = line[:ind].strip(), line[ind+2:].strip()
            if ', ' in s:
                s = re.sub(' +', ', ', s)  # e.g., 25030.html
                s = re.sub(',+', ',', s)
            for name in s.split(', '):  # e.g., 'man8-1.html'
                name = name.strip()
                if re.match(r'.*-[0-9]+(.[0-9]+)*$', name):  # e.g., 2to3-3.3
                    name = '-'.join(name.split('-')[:-1])
                if name != '':
                    name_summary_dict[name] = summ
                    _names.add(name)
            if lsnum == 0:
                lsnum = countLeftSpace(lines[i])
        elif line != '' and len(_names) > 0:
            for name in _names:
                name_summary_dict[name] += ' ' + line
        i += 1

    return name_summary_dict, lsnum


def extractRelatedCmds(btext):
    """
    Extract related cmds from a <see also> bytes-text.
    """
    cmdname_cmdhtml_dict, s = {}, '<a href="../man'
    ind = btext.find(s)
    while ind != -1:
        ind += len(s)
        end = btext.find('">', ind)
        cmdhtml = 'man' + btext[ind:end].strip()
        ind = end + 2
        end = btext.find('</a>', ind)
        cmdname = btext[ind:end].strip()
        cmdname_cmdhtml_dict[cmdname] = cmdhtml
        ind = btext.find(s, end)
    return cmdname_cmdhtml_dict


def extractTemplatesInSynopsis(text, cmdnames, name_lsnum):
    """
    Extract cmd templates from a <synopsis> text.
    """
    templates, lsnums = [], []
    ps, i, template, rest_ps = text.strip('\n').split('\n'), 0, '', []
    while i < len(ps):
        p = ps[i]
        lsnum = countLeftSpace(p)
        p = removeSpaces(p)
        v, u1, u2 = startsWithCmdName(p, cmdnames, '1')
        if not v and re.match('[a-z0-9][a-zA-Z0-9-_+]+ [\[<>{(-].*', p):
            v, u1 = True, [p.split(' ')[0]]  # e.g., 27823.html, 302.html
        if lsnum < name_lsnum or p == '' or v:  # e.g., 'UNIX-style usage' in tar
            if template != '':
                templates.append(template)
                template = ''
        if v:
            if u2 != '':  # correct spelling errors, e.g., desktop -> destkop
                p = p.replace(u2, u1[0], 1)
            lsnums.append(lsnum)
            template = ' => '.join(u1) + ' -> ' + p
        elif template != '':
            if template.endswith('=') or template.endswith('-'):
                template += p  # e.g., 10108.html
            else:
                template += ' ' + p
        else:  # for extracting paras, e.g., 12923.html, 302.html
            rest_ps.append(p)
        i += 1
    if template != '':
        templates.append(template)

    template_cmdnames_dict = {}
    for template in templates:
        sa = template.split(' -> ')
        temp, cmdnames = sa[1].strip(), sa[0].strip()
        if ' => ' not in cmdnames:
            template_cmdnames_dict[temp] = cmdnames
        else:  # e.g., 2csv.html
            _1st_token = temp.split()[0]
            for cmdname in cmdnames.split(' => '):
                rtemp = temp.replace(_1st_token, cmdname, 1)
                template_cmdnames_dict[rtemp] = cmdname
    del templates
    synopsis_lsnum = name_lsnum if len(lsnums) == 0 else sorted(lsnums)[0]

    return template_cmdnames_dict, synopsis_lsnum


def divideText(text, standard_lsnums):
    """
    Divide the text of a pre_node based on sub-labels.
    """
    label_parts_dict, label, parts, parts_2, i = {}, '', [], [], 0
    ps = [ p for p in text.split('\n\n') if p.strip('\n ') != '' ]
    min_lsnum, max_lsnum = standard_lsnums[0], standard_lsnums[-1]

    while i < len(ps):
        p = ps[i]
        _p = p.strip('\n ')
        lsnum = countLeftSpace(p)
        if lsnum < min_lsnum and not _p.startswith('-') and len(_p.split()) <= 5:  # e.g., 'Operation mode' in tar
            if len(parts) > 0 or len(parts_2) > 0:
                label_parts_dict[label] = parts + [part for part in parts_2 if parts_2 not in parts ]
                parts, parts_2 = [], []
            sa = p.strip('\n').split('\n')
            label = sa[0].strip('\n ')
            if len(sa) > 1:
                rest_p = '\n'.join(sa[1:])
                _lsnum = countLeftSpace(rest_p)
                if _lsnum in standard_lsnums:
                    parts.append(rest_p)
        elif lsnum in standard_lsnums: # or _p.split()[0] in cmdnames:  # to find more examples, e.g., 114.html
            parts.append(p)
        elif lsnum > max_lsnum and len(parts) > 0:
            parts[-1] = parts[-1] + '\n' + p
        if re.match('[\\\+\-]', _p): # possible paras that strat with '-', '\', or '+', e.g., 10012.html, 5397.html, find.html
            parts_2.append(p)
        i += 1

    if len(parts) > 0 or len(parts_2):
        label_parts_dict[label] = parts + [part for part in parts_2 if parts_2 not in parts ]
    return label_parts_dict


def extractParas(parts, btext, cmdnames):
    """
    Extract paras and their descriptions.
    """
    para_desc_dict = {}

    revised_parts, i = [], 0
    while i < len(parts):
        p1, p2 = parts[i].strip(), parts[i+1].strip() if i+1 < len(parts) else ''
        if (re.match('-[a-zA-Z0-9-_+=<>\[\].]+$', p1) and p2.startswith('-')) or \
                (re.match('[a-zA-Z-_+.]+$', p1) and re.match('[a-zA-Z-_+.]+$', p2)):
            revised_parts.append(p1 + ', ' + p2)  # e.g., 10109.html
            i += 2
        else:
            revised_parts.append(p1)
            i += 1

    for p in revised_parts:
        sa, i, revised_sa = p.strip('\n ').split('\n'), 0, []
        while i < len(sa):
            s = sa[i].strip()
            if s != '':  # to deal with 12923.html
                l = '' if len(revised_sa) == 0 else revised_sa[-1].strip()
                if l != '' and s.startswith('-') and \
                        re.match('-[a-zA-Z0-9-_+=<>\[\].]+(, -[a-zA-Z0-9-_+=<>\[\].]+)*$', l):
                    revised_sa[-1] = revised_sa[-1] + ', ' + s
                elif l != '' and re.match('[a-zA-Z-_+.]+$', s) and re.match('[a-zA-Z-_+.]+(, [a-zA-Z-_+.]+)*$', l):
                    revised_sa[-1] = revised_sa[-1] + ', ' + s
                else:
                    revised_sa.append(sa[i])
            i += 1

        # print('p:', p, '->', 'revised_sa:', revised_sa)
        para, desc, sa = '', '', revised_sa
        if len(sa) == 1:
            para, desc = identifyPara(sa[0], btext)
        elif len(sa) > 1:
            para, desc = identifyPara(sa[0], btext)
            desc = '\n'.join([desc] + sa[1:])
        if para != '':
            _1st_token = para.split()[0].lower()
            if _1st_token not in cmdnames:  # 'man ls' in man.html
                para_desc_dict[para] = removeSpaces(desc)

    return para_desc_dict


def identifyPara(line, btext):
    """
    Identify a para from a line.
    """
    if re.match('[\\\+][a-zA-Z0-9_]+( .*)?$', line):  # e.g., '\a' and '\0num' in 5397.html
        para = line.split()[0]
        return para, line.replace(para, '').strip()
    if re.match('[a-zA-Z0-9_\-]+,? -[a-z]+ [a-zA-Z0-9_\-]+( .*)?$', line):  # e.g., 'expr1 -and expr2' in find.html
        sa = line.split()
        if sa[0].endswith(','):
            sa[0] = sa[0][:-1]
        if '<u>' + sa[0] + '</u>' in btext and '<u>' + sa[2] + '</u>' in btext:
            para = ' '.join(sa[:3])
            return para, line.replace(para, '').strip()

    # line = line.replace('|', ',')
    line = removeSpaces(line.replace('·', ''))  # 12766.html
    line = line.replace('eXtract', 'Extract').replace(' ,', ',')  # e.g., 'x' in 7z
    if re.match('[a-zA-Z-_+.]+(, [a-zA-Z-_+.]+)*$', line) and \
            not re.match('.*[a-zA-Z0-9]\.$', line):  # 114.html, 1654.html
        return line, ''
    if re.match('-[a-zA-Z-_]+ [a-zA-Z0-9-_+. ]+$', line):  # 371.html
        sa = line.split()
        if '<u>' + ' '.join(sa[1:]) + '</u>' in btext:
            return line, ''
    if re.match('[a-zA-Z-_]+ ?= ?<.*>', line):  # man8-0.html
        return line, ''

    revised_line, u_tokens = line, set()
    for s in revised_line.split():
        if s.endswith(','):
            s = s[:-1]
        if '<u>' + s + '</u>' in btext:
            revised_line = revised_line.replace(s, '', 1)
            u_tokens.add(s)
        elif re.match('[A-Z]', s):
            break

    revised_line, para, flag = \
        re.sub(' +', ' ', revised_line).replace(' ,', ',').strip(', '), '', ''
    # print('revised_line:', revised_line)

    if revised_line != '':
        sa, b = revised_line.split(), False
        if len(sa) == 1 and re.match('[a-zA-Z0-9-_=+.]+$', revised_line):
            para = revised_line  # 1654.html
        elif len(sa) >= 2 and (re.match('[A-Z] [A-Z]', sa[0] + ' ' + sa[1]) or
                             re.match('[a-z-_]+ [A-Z]', sa[0] + ' ' + sa[1])):  # 'a' and 'rn' in 7z
            flag, b = sa[1], True
        elif re.match('[-+!(/@%$~]', sa[0]) or re.match('[A-Z0-9-_]+,?$', sa[0]):  # '-H', '! expr', 'LANG' in find
            b = True
            for i in range(len(sa)):  # '-f FIX, --fix=FIX', 'LANG, LC_MESSAGES' in man
                if re.match('[A-Z]', sa[i]) and not re.match('[A-Z0-9-_]+,?$', sa[i]):
                    flag = sa[i]
                    break
        if b:  # flag == '' means a para line
            para = line if flag == '' else line.partition(flag)[0]
    else:
        para = line

    para = para.strip()
    if para != '':
        _para = para  # filter incorrect paras
        for token in u_tokens:
            _para = _para.replace(token, '')
        if _para.endswith(':') or re.match('.*[a-zA-Z0-9]\.$', _para):
            para = ''
        elif para != '' and (re.match('.* [a-z]+[a-z-_]* [a-z]+', _para) or re.match('.* [A-Z0-9-_]+ [A-Z0-9-_]+', _para)):
            para = ''
        if para != '' and not _para.startswith('-') and ' -' in _para:
            para = ''
        if para != '' and re.match('[A-Z][A-Z0-9-_]+ .*', _para) and re.match('.* [a-z]', _para):
            para = ''

    # 27823.html, 12766.html, man8-0.html
    # if para == '' and (line.startswith('-') or re.match('[a-z-_]+ ?= ?<', line)):
    if para == '' and line.startswith('-'):
        line = line.replace('< ', '<').replace(' >', '>').replace(',', ' , ')
        line = re.sub(' +', ' ', line)
        sa, i, para = line.split(), 1, ''
        while i < len(sa):
            s = sa[i]
            if s in {'or', 'and', ','} and i+1 < len(sa) and sa[i+1].startswith('-'):
                para = ' '.join([para.strip(), s, sa[i+1]])
                i += 2
            elif s.startswith('-') or re.match('.*[<>\[\]=].*', s):
                para += s + ' '
                i += 1
            else:
                break
        para = sa[0] + ' ' + para.strip()

    para = para.strip()
    if para.startswith('-<') and not para.endswith('>'):  # e.g., '-<The' 114.html
        para = ''
    if para != '':
        desc = line.replace(para, '', 1).strip()
        if para.endswith(' -') and re.match('[a-z]', desc):  # 15297.html, except 389-console.html
            para = para[:-2].strip()
        elif ' - ' in para:
            para = para.split(' - ')[0].strip()
        return para, desc
    return '', ''


def startsWithCmdName(line, cmdnames, case_type):
    """
    Check whether a line string starts with any cmds' names.
    case_type == 1: for extracting cmd templates
    case_type == 2: for extracting cmd examples
    """
    line = removeSpaces(line.lower())
    if line.startswith('$'):  # 4s-cluster-start.1J
        line = line[1:].strip()

    _1st_token = line.split(' ')[0]
    if _1st_token in cmdnames:
        return True, [_1st_token], ''
    if '/' in _1st_token:  # e.g., man8-1.html
        sa = _1st_token.split('/')
        if sa[-1] in cmdnames:
            return True, [sa[-1]], ''
    if _1st_token.startswith('<'):  # e.g., 2csv.html
        sa = set(_1st_token[1:-1].split('|'))
        inters = sa.intersection(cmdnames)
        if len(inters) > 0:
            return True, sorted(inters), ''

    if case_type == '1':  # e.g., '0destkop' in 0.html
        if len(_1st_token) >= 8:
            d, s = minEditDistance(_1st_token, cmdnames)
            if d <= 2:  # _1st_token is possibly wrong in this case
                return True, [s], _1st_token
        sa = line.split()
        if len(sa) >= 2:  # e.g., 'gbp  create-remote-repo' in 10108.html
            _token = sa[0] + ' ' + sa[1]
            d, s = minEditDistance(_token, cmdnames)
            if d <= 2:  # _token is posiibly right in this case
                return True, [_token], ''

    if case_type == '2' and '|' in line:  # e.g., 1000.html
        for s in line.split('|'):
            s = s.strip()
            if s == '':
                continue
            _1st_token = s.split()[0]
            if _1st_token in cmdnames:
                return True, '', ''

    return False, '', ''


def countLeftSpace(s):
    """
    Count # spaces on the left side of a string.
    """
    s = s.lstrip('\n')
    return len(s) - len(s.lstrip())


def removeSpaces(s):
    """
    Remove redundant spaces.
    """
    return re.sub(' +', ' ', s.strip('\n '))


def minEditDistance(s, sa):
    """
    Compute the min-edit distenace between a string and all strings in a set.
    """
    mindis, mins = 1000, ''
    for i in sa:
        d = Levenshtein.distance(s, i)
        if d < mindis:
            mindis, mins = d, i
    return mindis, mins


def collectMPCmds(manual_dir, parsed_manual_dir):
    """
    Collect cmds for each ASE2021_release.
    """
    all_cmdnames = set()

    for release in releases:
        cmds_dict = {}
        for i in [1, 8]:
            man = 'man' + str(i)
            s = '/' + release + '/' + man
            print(s)
            man_json_dir, man_html_dir = \
                parsed_manual_dir + s + '-json', manual_dir + s + '-html'
            for name in os.listdir(man_json_dir):
                cmdjson, _id = man_json_dir + '/' + name, name[:-5]
                cmd_dict = readJson(cmdjson)
                name_summ_dict, name_templates_dict, descs = \
                    cmd_dict['cmd summary'], cmd_dict['cmd templates'], cmd_dict['cmd description']
                described_paras, related_cmds = cmd_dict['described paras'], cmd_dict['related cmds']
                cmdnames = set(name_summ_dict.keys()) | set(name_templates_dict.keys())
                for cmdname in cmdnames:
                    if validCmdName(cmdname):
                        paras_dict, refined_descs = {}, []
                        for label in described_paras:
                            for item in described_paras[label]:
                                paras_dict[item['paras']] = {
                                    'label': label, 'description': item['description']
                                }
                        for desc in descs:
                            if cmdname in desc:
                                refined_descs.append(desc)
                        for cmd in related_cmds:
                            link = related_cmds[cmd]
                            related_cmds[cmd] = link if not link.startswith('man') else \
                                conf.ubuntu_mp_url + '/' + release + '/en/' + link
                        all_cmdnames.add(cmdname)
                        cmdname_id = cmdname + '_' + _id  # duplicate cmds in different files, e.g., echo
                        if cmdname_id not in cmds_dict:
                            cmds_dict[cmdname_id] = {}
                        cmds_dict[cmdname_id][release + '/' + man] = {
                            'cmd': cmdname,
                            'ASE2021_release': release,
                            'ASE2021_release version': release_vers[releases.index(release)],
                            'man': i, 'cmd html': man_html_dir + '/' + name[:-5] + '.html', 'cmd json': cmdjson,
                            'cmd description': refined_descs,
                            'cmd summary': '' if cmdname not in name_summ_dict else name_summ_dict[cmdname],
                            'cmd templates': [] if cmdname not in name_templates_dict else name_templates_dict[cmdname],
                            'described paras': paras_dict, 'related cmds': related_cmds
                        }
        writeJson(cmds_dict, parsed_manual_dir + '/' + release + '_cmds.json')
        del cmds_dict

    with open(parsed_manual_dir + '/_all_cmdnames.txt', 'w', encoding='utf-8') as f:
        for cmdname in sorted(all_cmdnames):
            f.write(cmdname + '\n')


def validCmdName(cmdname):
    """
    Check whether a cmd name is valid or not.
    """
    if re.match('[a-zA-Z0-9].*[a-zA-Z0-9+\-]$', cmdname) and \
            not re.match('[0-9.\-_+]+$', cmdname) and len(cmdname.split()) < 3:
        return True
    return False


if __name__ == '__main__':

    # _test_mps_dir = r'test_mps'
    # for _name in os.listdir(_test_mps_dir):
    #     if _name.endswith('.html'):
    #         print('\n\n\nparse:', _name, '=====')
    #         parseCmdHtml(_test_mps_dir + '/' + _name, '', _test_mps_dir + '/' + _name.replace('.html', '.json'))

    _parsed_manuals_dir = conf.exp_manual_dir + '/_parsed'
    if not os.path.exists(_parsed_manuals_dir):
        os.makedirs(_parsed_manuals_dir)

    start = time.time()
    parseCmdHtmls(conf.exp_manual_dir, _parsed_manuals_dir)  # 2572s
    collectMPCmds(conf.exp_manual_dir, _parsed_manuals_dir)  # 927s
    print(time.time() - start, 's')
