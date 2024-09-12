#!/usr/bin/env python3
from datetime import datetime
import uuid
from src.dt_dbms import Dbms, get_new_dbms

class MetaDataBasic:
    TERMINAL_ID = None
    TABLE_NAME = None
    META_TYPE = None
    META_IDENT = None
    REQUISITES = {'id': 'id'}

    def __init__(self, id = None):
        self.reset_requisites_value()

        self.id = id

    def set_requisites_value(self,o,r):
        for req, col in self.REQUISITES.items():
            setattr(o, req, r[col])

    def get_requisites_value(self):
        requisites = {}
        for req, column in self.REQUISITES.items():
            requisites[column] = getattr(self,req)

        return requisites

    def reset_requisites_value(self):
        for req in self.REQUISITES:
            setattr(self, req, None)

    def save(self,keys):
        if Dbms.insert_update(self.TABLE_NAME,self.get_requisites_value(),keys) is not False:
            return True
        else:
            return  False

    @classmethod
    def set_table_name(cls, value):
        cls.TABLE_NAME = value

    @classmethod
    def set_meta_type(cls, value):
        cls.META_TYPE = value

    @classmethod
    def set_meta_ident(cls, value):
        cls.META_IDENT = value

    @staticmethod
    def connect():
        return Dbms.connect()

    @staticmethod
    def disconnect():
        return Dbms.disconnect()

    @staticmethod
    def set_connection_params(**kwargs):
        return Dbms.set_dbms_attribute(**kwargs)

    @staticmethod
    def get_new_dbms(**kwargs):
        return get_new_dbms(**kwargs)

    @classmethod
    def set_terminal_id(cls):
        res = Dbms.get_terminal_id()

        if res is None:
            cls.TERMINAL_ID = None

            res = False
        elif res:
            cls.TERMINAL_ID = res

            res = True

        return res

    @classmethod
    def get_terminal_id(cls):
        return cls.TERMINAL_ID

class MetaData(MetaDataBasic):
    def find(self,row_id):
        res = Dbms.find(self.TABLE_NAME, {'id': row_id})

        if res:
            self.set_requisites_value(self,res[0])

        return res

    def find_by_requisite(self,req,val):
        records = Dbms.find(self.TABLE_NAME,{self.REQUISITES[req]: val})

        res = []

        if records:
            for r in records:
                o = self.__new__(self.__class__)

                self.set_requisites_value(o,r)

                res.append(o)

        if len(res) == 0:
            return False
        else:
            return res

    def save(self):
        if super().save(['id']) is not False:
            return True
        else:
            return  False


class MetaDataWithItems(MetaData):
    ITEMS_TABLES = {}

    def add_item(self, meta_ident):
        res = None

        cls = self.ITEMS_TABLES[meta_ident]

        if cls:
            res = cls(self.id)

        return res

    def get_item(self, meta_ident, position):
        res = None

        cls = self.ITEMS_TABLES[meta_ident]

        if cls:
            res = cls(self.id, position)

            return res.get_item(position)

        return res

class MetaDataItems(MetaDataBasic):
    REQUISITES = MetaDataBasic.REQUISITES.copy()
    REQUISITES['position'] = 'position'

    def __init__(self, id, position = None):
        super().__init__()

        self.id = id

        if position is None:
            if self.position is None:
                self.position = self.items_count() + 1
            else:
                self.position = max(self.items_count(),self.position) + 1
        else:
            self.position = position

    def items_count(self):
        res = 0

        if self.id:
            res = Dbms.items_count(self.TABLE_NAME, self.id)

            if res:
                res = res[0]['max_pos']

            if res is None:
                res = 0

        return res

    def add_item(self):
        return self.__new__(self.__class__, self.id)

    def get_item(self,position):
        res = Dbms.find(self.TABLE_NAME, {'id': self.id, 'position': position})

        if res:
            items = self.__new__(self.__class__, self.id)

            items.set_requisites_value(items,res[0])

            return items

        return res

    def save(self):
        if super().save(['id','position']) is not False:
            return True
        else:
            return False


class DocumentWorksItemsOperators(MetaDataItems):
    META_IDENT = 'ItemsOperators'
    TABLE_NAME = 'dt_doc_works_items_operators'

    REQUISITES = MetaDataItems.REQUISITES.copy()
    REQUISITES['operator_id'] = 'dt_operators_id'

class DocumentWorksItemsIntervals(MetaDataItems):
    META_IDENT = 'ItemsIntervals'
    TABLE_NAME = 'dt_doc_works_items_intervals'

    REQUISITES = MetaDataItems.REQUISITES.copy()
    REQUISITES['begin'] = 'begin_timestamp'
    REQUISITES['end'] = 'end_timestamp'
    REQUISITES['work_interval'] = 'work_interval'
    REQUISITES['stop_interval'] = 'stop_interval'

class Reference(MetaData):
    META_TYPE = 'Reference'

    REQUISITES = MetaData.REQUISITES.copy()
    REQUISITES['name'] = 'name'


class ReferenceOperators(Reference):
    META_IDENT = 'Operators'
    TABLE_NAME = 'dt_operators'

class ReferenceSpecialCodes(Reference):
    META_IDENT = 'SpecialCodes'
    TABLE_NAME = 'dt_special_codes'

class ReferenceTerminals(Reference):
    META_IDENT = 'Terminals'
    TABLE_NAME = 'dt_terminals'

    REQUISITES = Reference.REQUISITES.copy()
    REQUISITES['equipment_id'] = 'dt_equipments_id'
    REQUISITES['doc_tasks_id'] = 'dt_doc_tasks_id'
    REQUISITES['doc_works_id'] = 'dt_doc_works_id'
    REQUISITES['last_seen'] = 'last_seen'


class ReferenceEquipments(Reference):
    META_IDENT = 'Equipments'
    TABLE_NAME = 'dt_equipments'

    REQUISITES = Reference.REQUISITES.copy()
    REQUISITES['line_id'] = 'line_id'


class Document(MetaDataWithItems):
    META_TYPE = 'Document'

    REQUISITES = MetaDataWithItems.REQUISITES.copy()
    REQUISITES['timestamp'] = 'doc_timestamp'

    @classmethod
    def new_document(cls):
        res = cls()
        res.id =  uuid.uuid4()
        res.timestamp = datetime.now()

        return res


class DocumentTasks(Document):
    META_IDENT = 'Tasks'
    TABLE_NAME = 'dt_doc_tasks'

    REQUISITES = Document.REQUISITES.copy()
    REQUISITES['doc_number'] = 'doc_number'
    REQUISITES['line_id'] = 'line_id'
    REQUISITES['status'] = 'status'


class DocumentWorks(Document):
    META_IDENT = 'Works'
    TABLE_NAME = 'dt_doc_works'

    REQUISITES = Document.REQUISITES.copy()
    REQUISITES['task_id'] = 'dt_doc_tasks_id'
    REQUISITES['terminal_id'] = 'dt_terminals_id'
    REQUISITES['status'] = 'doc_status'

    ITEMS_TABLES = {DocumentWorksItemsOperators.META_IDENT: DocumentWorksItemsOperators, DocumentWorksItemsIntervals.META_IDENT: DocumentWorksItemsIntervals}
