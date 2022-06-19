import os

from conf import conf
from file_utils import readJson, writeJson
from post_preprocesser import preprocessStr


def analyzeTLDRCmds(tldr_cmds_json, res_json):
    """
    Analyze the cmds extracted from TLDR tutorials.
    """
    cmds_dict, tr_cmds_dict, common_cmdnum, linux_cmdnum = \
        readJson(tldr_cmds_json), {}, 0, 0

    for cmdname in cmds_dict:
        for category in cmds_dict[cmdname]:
            if category in { 'common', 'linux' }:
                if category == 'common':
                    common_cmdnum += 1
                if category == 'linux':
                    linux_cmdnum += 1
                summ = cmds_dict[cmdname][category]['Explanation']
                task_script_dict = cmds_dict[cmdname][category]['Examples']
                tr_cmds_dict[category + '_' + cmdname] = {
                    'Category': category,
                    'Command': cmdname,
                    'Summary': summ,
                    'Task-Script': task_script_dict,
                    'P-Summary': preprocessStr(summ, '2'),
                    'P-Tasks': preprocessStr(' '.join(task_script_dict.keys()), '2')
                }

    print('# cmds in common:', common_cmdnum)  # 1264
    print('# cmds in linux:', linux_cmdnum)  # 533
    print('# all cmds in TLDR:', len(tr_cmds_dict))  # 1797
    writeJson(tr_cmds_dict, res_json)


if __name__ == '__main__':

    _all_tldrcmds_json = conf.exp_tldr_dir + '/all_tldrcmds.json'
    _linux_tldrcmds_json = conf.exp_tldr_dir + '/linux_tldrcmds.json'

    analyzeTLDRCmds(_all_tldrcmds_json, _linux_tldrcmds_json)
