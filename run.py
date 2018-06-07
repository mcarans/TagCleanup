import logging
import fnmatch
from os.path import join, expanduser

from hdx.data.hdxobject import HDXError
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
    if tags_lower_five == 'merge' or tags_lower_five == 'split' or (';' not in final_tags and len(final_tags) > 50):
        logger.error('%s%s - Invalid final tag!' % (text, final_tags))
        return
    if remove_existing:
        dataset.remove_tag(tag)
    tags = ', '.join(dataset.get_tags())
    if dataset.add_tags(final_tags.split(';')):
        logger.info('%s%s! Dataset tags: %s' % (text, final_tags, tags))
    else:
        logger.warning('%s%s - At least one of the tags already exists! Dataset tags: %s' % (text, final_tags, tags))


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


def update_dataset_tags(dataset, tags_dict, wildcard_tags):
    changed = False

    for tag in dataset.get_tags():
        if tag in tags_dict.keys():
            if do_action(tags_dict, dataset, tag, tag):
                changed = True
        else:
            for wildcard_tag in wildcard_tags:
                if fnmatch.fnmatch(tag, wildcard_tag):
                    if do_action(tags_dict, dataset, tag, wildcard_tag):
                        changed = True
    return changed


def read_tags_spreadsheet(url):
    with Download() as downloader:
        tags_dict = downloader.download_tabular_rows_as_dicts(url, keycolumn=5)

        wildcard_tags = list()
        for tag in tags_dict.keys():
            if '*' in tag:
                wildcard_tags.append(tag)

        return tags_dict, wildcard_tags


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()
    tags_dict, wildcard_tags = read_tags_spreadsheet(configuration['tags_url'])

    for dataset in Dataset.get_all_datasets(check_duplicates=False): # [Dataset.read_from_hdx('global-acute-malnutrition-prevalence-of-sahel-countries')]: #
        changed = update_dataset_tags(dataset, tags_dict, wildcard_tags)

        if changed:
            if dataset.get_tags():
                if real_run:
                    try:
                        dataset.update_in_hdx(update_resources=False, hxl_update=False)
                    except HDXError as ex:
                        logger.exception(ex)
            else:
                if dataset['private']:
                    privatepublic = 'private'
                else:
                    privatepublic = 'public'
                logger.warning('%s (%s) has no tags!' % (dataset['name'], privatepublic))


if __name__ == '__main__':
    facade(main, hdx_site='demo', user_agent_config_yaml=join(expanduser('~'), '.tagcleanupuseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))
