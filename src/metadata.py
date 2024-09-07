#!/usr/bin/env python3
from datetime import datetime
import uuid
from src.dt_dbms import Dbms, get_new_dbms

class MetaData:
    TERMINAL_ID = None
    REQUISITES = {'id': 'id'}

    def __init__(self,table_name,meta_type,meta_ident):
        self.table_name = table_name
        self.meta_type = meta_type
        self.meta_ident = meta_ident

        self.id = None

    def set_requsites_value(self,o,r):
        for req, col in self.REQUISITES.items():
            setattr(o, req, r[col])

    def find(self,row_id):
        res = Dbms.find(self.table_name, row_id)

        if res:
            self.set_requsites_value(self,res[0])

        return res

    def find_by_requsite(self,req,val):
        records = Dbms.find_by_requisite(self.table_name,{self.REQUISITES[req]: val})

        res = []

        if records:
            for r in records:
                o = self.__new__(self.__class__)

                self.set_requsites_value(o,r)

                res.append(o)

        if len(res) == 0:
            return False
        else:
            return res

    def save(self):
        requisites = {}
        for req, column in self.REQUISITES.items():
            requisites[column] = getattr(self,req)

        if Dbms.insert_update(self.table_name,requisites) is not False:
            return True
        else:
            return  False

    @staticmethod
    def connect():
        return Dbms.connect()

    @staticmethod
    def disconnect():
        return Dbms.disconnect()

    @staticmethod
    def set_adress(value):
        return Dbms.set_dbms_attribute(adress=value)

    @staticmethod
    def set_port(value):
        return Dbms.set_dbms_attribute(port=value)

    @staticmethod
    def set_dbname(value):
        return Dbms.set_dbms_attribute(dbname=value)

    @staticmethod
    def set_user(value):
        return Dbms.set_dbms_attribute(user=value)

    @staticmethod
    def set_password(value):
        return Dbms.set_dbms_attribute(password=value)

    @classmethod
    def set_terminal_id(cls):
        cls.TERMINAL_ID = Dbms.get_terminal_id()

    @staticmethod
    def get_new_dbms(**kwargs):
        return get_new_dbms(**kwargs)


class Reference(MetaData):
    REQUISITES = MetaData.REQUISITES.copy()
    REQUISITES['name'] = 'name'

    def __init__(self, table_name, meta_ident):
        super().__init__(table_name,'Reference',meta_ident)

        self.name = ''


class ReferenceOperators(Reference):
    def __init__(self):
        super().__init__('dt_operators','Operators')

class ReferenceSpecialCodes(Reference):
    def __init__(self):
        super().__init__('dt_special_codes','SpecialCodes')

class ReferenceTerminals(Reference):
    REQUISITES = Reference.REQUISITES.copy()
    REQUISITES['equipment_id'] = 'dt_equipments_id'
    REQUISITES['doc_tasks_id'] = 'dt_doc_tasks_id'
    REQUISITES['doc_works_id'] = 'dt_doc_works_id'
    REQUISITES['last_seen'] = 'last_seen'

    def __init__(self):
        super().__init__('dt_terminals','Terminals')

        self.equipment_id = None
        self.doc_tasks_id = None
        self.doc_works_id = None
        self.last_seen = None


class ReferenceEquipments(Reference):
    REQUISITES = Reference.REQUISITES.copy()
    REQUISITES['line_id'] = 'line_id'

    def __init__(self):
        super().__init__('dt_equipments','Equipments')

        self.line_id = None


class Document(MetaData):
    REQUISITES = MetaData.REQUISITES
    REQUISITES['timestamp'] = 'doc_timestamp'

    def __init__(self, table_name, meta_ident):
        super().__init__(table_name,'Document',meta_ident)

        self.timestamp = None

    @classmethod
    def new_document(cls):
        res = cls()
        res.id =  uuid.uuid4()
        res.timestamp = datetime.now()

        return res


class DocumentTasks(Document):
    REQUISITES = Document.REQUISITES.copy()
    REQUISITES['doc_number'] = 'doc_number'
    REQUISITES['line_id'] = 'line_id'

    def __init__(self):
        super().__init__('dt_doc_tasks','Tasks')

        self.doc_number = ''
        self.line_id = None


class DocumentWorks(Document):
    REQUISITES = Document.REQUISITES.copy()
    REQUISITES['task_id'] = 'dt_doc_tasks_id'
    REQUISITES['status'] = 'doc_status'

    def __init__(self):
        super().__init__('dt_doc_works','Works')

        self.task_id = None
        self.status = None