'''
Migrate all the FileFields on a given Model to a new Storage backend.
'''
import logging

from optparse import make_option

from django.conf import settings
from django.core.management.base import LabelCommand
from django.core.files.storage import get_storage_class, default_storage
from django.db.models import FileField, get_model
from django.core.files.storage import FileSystemStorage


OLD_STORAGE = getattr(settings, 'OLD_STORAGE', {})
#: The storage engine where everything was stored in.
OLD_DEFAULT_FILE_STORAGE = getattr(settings, 'OLD_DEFAULT_FILE_STORAGE', default_storage)
if isinstance(OLD_DEFAULT_FILE_STORAGE, str):
    OLD_DEFAULT_FILE_STORAGE = get_storage_class(OLD_DEFAULT_FILE_STORAGE)()

NEW_STORAGE = getattr(settings, 'NEW_STORAGE', {})
#: The storage engine where everything will be stored in.
NEW_DEFAULT_FILE_STORAGE = getattr(settings, 'NEW_DEFAULT_FILE_STORAGE', default_storage)
if isinstance(NEW_DEFAULT_FILE_STORAGE, str):
    NEW_DEFAULT_FILE_STORAGE = get_storage_class(NEW_DEFAULT_FILE_STORAGE)()


class Command(LabelCommand):
    args = '<app_name.Model app_name.Model2 ...>'
    label = 'model (app_name.ModelName)'
    help = __doc__

    option_list = LabelCommand.option_list + (
        make_option(
            '--overwrite', '-f', action='store_true', dest='overwrite',
            help='Overwrite file that exist in the new storage backend'
        ),
        make_option(
            '--to-new', action='store_true', dest='to_new',
            help='Copy files from the current storage backend to the new storage backend'
        ),
        make_option(
            '--path', '-p', dest='path', default=settings.MEDIA_ROOT,
            help=''
        ),
    )

    def handle_label(self, label, **options):
        app_label, model_name = label.split('.')
        model_class = get_model(app_label, model_name)

        if model_class is None:
            return 'Skipped %s. Model not found.' % label

        field_names = []
        old_storages = {}

        # Find file fields in models
        for field in model_class._meta.fields:
            if isinstance(field, FileField):
                field_names.append(field.name)
                field_path = '%s.%s' % (label, field.name)
                if options['to_new']:
                    if field_path in NEW_STORAGE:
                        old_storages[field_path] = NEW_STORAGE[field_path]
                    else:
                        old_storages[field_path] = NEW_DEFAULT_FILE_STORAGE
                else:
                    if field_path in OLD_STORAGE:
                        old_storages[field_path] = OLD_STORAGE[field_path]
                    else:
                        old_storages[field_path] = OLD_DEFAULT_FILE_STORAGE

        item_index = 1
        item_count = model_class._default_manager.count()

        # copy the files for all the models
        for instance in model_class._default_manager.all():
            logging.debug('Handling "%s"' % instance)
            # check all field names
            for fn in field_names:
                print '[{0} of {1}]'.format(item_index, item_count),
                item_index += 1

                field = getattr(instance, fn)
                if options['to_new']:
                    new_storage = old_storages['%s.%s' % (label, fn)]
                    old_storage = field.storage
                else:
                    old_storage = old_storages['%s.%s' % (label, fn)]
                    new_storage = field.storage

                if field.name == '':
                    logging.debug('Field is empty, ignoring file.')
                    print
                elif new_storage == old_storage:
                    logging.debug('Same storage engine, ignoring file.')
                    print
                # do we have multiple files?
                elif hasattr(field, 'names'):
                    for name in field.names:
                        self.copy_file(new_storage, name, options)
                else:
                    self.copy_file(new_storage, field.name, options)
        return ''

    def copy_file(self, new_storage, filename, options):
        '''
        Copies the file between storage engines.

        .. note:: If ``DEBUG`` is still ``True``, we won't copy *anything*.

        :param django.core.files.storage.Storage new_storage: the storage
            engine to which the files will be copied
        :param str filename: the file we're copying
        :param dict options: the options of the command
        '''
        old_storage = FileSystemStorage(location=options['path'])

        print filename, old_storage.exists(filename), new_storage.exists(filename)

        # check whether file exists in old storage
        if not old_storage.exists(filename):
            logging.info('File doesn\'t exist in old storage, ignoring file.')
        # check wether file alread exists in the new storage
        elif not options['overwrite'] and new_storage.exists(filename):
            logging.info('File already exists in storage, ignoring file.')
        else:
            f = old_storage.open(filename)
            new_storage.save(filename, f)
            logger.info('Copying file "%s" to new storage.' % filename)
