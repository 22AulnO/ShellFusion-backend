import os
import time

from file_utils import writeJson
from conf import conf


def parseCmds(pages_dir, res_json):
    """
    Parse the commands supported by the TLDR project.
    NOTE: there are duplicate commands in different categories (e.g., common, linux, ...).
    """
    cmds_dict, category_cmds_dict = {}, {}

    for name in os.listdir(pages_dir):
        category = name  # e.g., common, linux, osx, ...
        cate_dir = pages_dir + '/' + name
        category_cmds_dict[category] = set()

        for md in os.listdir(cate_dir):
            explanation, link, task, examples = '', '', '', {}
            with open(cate_dir + '/' + md, encoding='utf-8') as f:
                for line in f.readlines():
                    line = line.strip()
                    if line == '':
                        continue
                    if line.startswith('# '):
                        cmdname = line[2:].strip()
                    elif line.startswith('>'):
                        if 'More information' not in line:
                            explanation += line[1:].strip() + '\n'
                        else:
                            sa = line.split('<')
                            if len(sa) == 2:
                                link = sa[1].replace('>.', '').strip()
                    elif line.startswith('-'):
                        task = line[1:-1].strip()
                    elif line.startswith('`'):
                        examples[task] = line[1:-1].strip()

            category_cmds_dict[category].add(cmdname)
            if cmdname not in cmds_dict:
                cmds_dict[cmdname] = {}
            cmds_dict[cmdname][category] = {
                'Explanation': explanation.strip(), 'More Information Link': link, 'Examples': examples
            }

    print('# all cmds:', len(cmds_dict))  # 1,955
    writeJson(cmds_dict, res_json)


if __name__ == '__main__':

    if not os.path.exists(conf.exp_tldr_dir):
        os.makedirs(conf.exp_tldr_dir)

    _all_tldrcmds_json = conf.exp_tldr_dir + '/all_tldrcmds.json'

    start = time.time()
    parseCmds(conf.tldr_dir + '/pages', _all_tldrcmds_json)  # 6.74s
    print(time.time() - start, 's')
