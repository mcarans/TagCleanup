import logging
from os.path import join, expanduser

from hdx.data.hdxobject import HDXError
from hdx.data.dataset import Dataset

from hdx.facades import logging_kwargs
logging_kwargs.update({'logging_config_yaml': join('config', 'logging_configuration.yml')})
from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)

real_run = False  # set to True to make changes


def main():
    """Generate dataset and create it in HDX"""
    for dataset in Dataset.get_all_datasets(check_duplicates=False):  # [Dataset.read_from_hdx('malawi-other')]:
        changed, error = dataset.clean_dataset_tags()

        if changed and not error:
            if real_run:
                try:
                    logger.info('%s: Updating dataset in HDX' % dataset['name'])
                    dataset['batch_mode'] = 'KEEP_OLD'
                    dataset['skip_validation'] = True
                    dataset.update_in_hdx(update_resources=False, hxl_update=False)
                except HDXError as ex:
                    logger.exception(ex)
            if not dataset.get_tags():
                if dataset['private']:
                    privatepublic = 'private'
                else:
                    privatepublic = 'public'
                logger.warning('%s (%s) has no tags!' % (dataset['name'], privatepublic))


if __name__ == '__main__':
    facade(main, hdx_site='feature', user_agent_config_yaml=join(expanduser('~'), '.tagcleanupuseragent.yml'))
