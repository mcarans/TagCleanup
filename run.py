import logging
import fnmatch
from os.path import join, expanduser

from hdx.utilities.downloader import Download
from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration

from hdx.facades import logging_kwargs
logging_kwargs.update({'logging_config_yaml': join('config', 'logging_configuration.yml')})
from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)

real_run = False  # set to True to make changes


def delete_tag(dataset, tag):
    logger.info('%s - Deleting tag %s!' % (dataset['name'], tag))
    dataset.remove_tag(tag)


def update_tag(dataset, tag, final_tags, wording, remove_existing=True):
    text = '%s - %s: %s -> ' % (dataset['name'], wording, tag)
    if not final_tags:
        if remove_existing:
            logger.error('%snothing!' % text)
        else:
            logger.warning('%snothing!' % text)
        return
    tags_lower_five = final_tags[:5].lower()
    if tags_lower_five == 'merge' or tags_lower_five == 'split' or (';' not in final_tags and len(final_tags) > 40):
        logger.error('%s%s - Invalid final tag!' % (text, final_tags))
        return
    if dataset.add_tags(final_tags.split(';')):
        if remove_existing:
            dataset.remove_tag(tag)
    logger.info('%s%s! Dataset tags: %s' % (text, final_tags, ', '.join(dataset.get_tags())))


def do_action(tags_dict, dataset, tag, tags_dict_key):
    whattodo = tags_dict[tags_dict_key]
    action = whattodo[u'action']
    final_tags = whattodo[u'final tags (semicolon separated)']
    changed = True
    if action == u'Delete':
        delete_tag(dataset, tag)
    elif action == u'Merge':
        update_tag(dataset, tag, final_tags, 'Merging')
    elif action == u'Fix spelling':
        update_tag(dataset, tag, final_tags, 'Fixing spelling')
    elif action == u'Non English':
        update_tag(dataset, tag, final_tags, 'Anglicising', remove_existing=False)
    else:
        changed = False
    return changed


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()

    with Download() as downloader:
        tags_dict = downloader.download_tabular_rows_as_dicts(configuration['tags_url'], keycolumn=5)
        tags = tags_dict.keys()

        wildcard_tags = list()
        for tag in tags:
            if '*' in tag:
                wildcard_tags.append(tag)

        for dataset in Dataset.get_all_datasets(check_duplicates=False):
            changed = False

            for tag in dataset.get_tags():
                if tag in tags:
                    changed = do_action(tags_dict, dataset, tag, tag)
                else:
                    for wildcard_tag in wildcard_tags:
                        if fnmatch.fnmatch(tag, wildcard_tag):
                            changed = do_action(tags_dict, dataset, tag, wildcard_tag)

            if changed:
                if dataset.get_tags():
                    if real_run:
                        dataset.update_in_hdx(update_resources=False, hxl_update=False)
                else:
                    if dataset['private']:
                        privatepublic = 'private'
                    else:
                        privatepublic = 'public'
                    logger.warning('%s (%s) has no tags!' % (dataset['name'], privatepublic))


if __name__ == '__main__':
    facade(main, hdx_site='test', user_agent_config_yaml=join(expanduser('~'), '.tagcleanupuseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))
