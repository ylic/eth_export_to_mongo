# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
import pymongo as pm
import json
import csv
import io
import threading
from json import JSONEncoder

import six


class BaseItemExporter(object):
    
    def __init__(self, **kwargs):
        self._configure(kwargs)

    def _configure(self, options, dont_fail=False):
        """Configure the exporter by poping options from the ``options`` dict.
        If dont_fail is set, it won't raise an exception on unexpected options
        (useful for using with keyword arguments in subclasses constructors)
        """
        self.encoding = options.pop('encoding', None)
        self.fields_to_export = options.pop('fields_to_export', None)
        self.export_empty_fields = options.pop('export_empty_fields', False)
        self.indent = options.pop('indent', None)
        self.db_name = options.pop('db_name', None)
        if not dont_fail and options:
            raise TypeError("Unexpected options: %s" % ', '.join(options.keys()))

    def export_item(self, item):
        raise NotImplementedError

    def serialize_field(self, field, name, value):
        serializer = field.get('serializer', lambda x: x)
        return serializer(value)

    def start_exporting(self):
        pass

    def finish_exporting(self):
        pass

    def _get_serialized_fields(self, item, default_value=None, include_empty=None):
        """Return the fields to export as an iterable of tuples
        (name, serialized_value)
        """
        if include_empty is None:
            include_empty = self.export_empty_fields
        if self.fields_to_export is None:
            if include_empty and not isinstance(item, dict):
                field_iter = six.iterkeys(item.fields)
            else:
                field_iter = six.iterkeys(item)
        else:
            if include_empty:
                field_iter = self.fields_to_export
            else:
                field_iter = (x for x in self.fields_to_export if x in item)

        for field_name in field_iter:
            if field_name in item:
                field = {} if isinstance(item, dict) else item.fields[field_name]
                value = self.serialize_field(field, field_name, item[field_name])
            else:
                value = default_value

            #处理长整形
            if type(value) is int :
                value = float(value)

            yield field_name, value

class MongoItemExporter(BaseItemExporter):
    
    def __init__(self, **kwargs):

        self._configure(kwargs, dont_fail=True)
        
    def get_content(self, item):
 
        fields = self._get_serialized_fields(item, default_value='',
                                             include_empty=True)
        lst = dict(fields)
        return lst




class CompositeItemExporter:
    def __init__(self, field_mapping):
        self.field_mapping = field_mapping

        self.exporter_mapping = {}
        self.counter_mapping = {}
        self.mongo_exporter = {}

    def open(self):

        for item_type, filename in self.field_mapping.items():
            
                fields = self.field_mapping[item_type]
                item_exporter = MongoItemExporter(fields_to_export=fields,db_name=item_type)
                self.mongo_exporter[item_type] = item_exporter



    def get_export(self,item):
        item_type = item.get('type')
    
        if item_type is None:
            raise ValueError('type key is not found in item {}'.format(repr(item))) 

        print(self.mongo_exporter)

        mongo_exporter = self.mongo_exporter[item_type]
        if mongo_exporter is None:
            raise ValueError('Exporter for item mongo_exporter not found')

        return mongo_exporter        
