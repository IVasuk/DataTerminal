#!/usr/bin/env python3
from src.dt_dbms import Dbms

class MetaData:
    TERMINAL_ID = None

    def __init__(self,table_name,meta_type,meta_ident):
        self.table_name = table_name
        self.meta_type = meta_type
        self.meta_ident = meta_ident

        self.id = None

    def find(self,id):
        return Dbms.find(self.table_name,id)

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

class Reference(MetaData):
    def __init__(self, table_name, meta_ident):
        super().__init__(table_name,'Reference',meta_ident)

        self.name = ''

    def find(self, id):
        res = super().find(id)

        if res:
            self.id = res[0].id
            self.name = res[0].name
        else:
            self.id = ''
            self.name = ''

        return res

class ReferenceOperators(Reference):
    def __init__(self):
        super().__init__('dt_operators','Operators')

class ReferenceSpecialCodes(Reference):
    def __init__(self):
        super().__init__('dt_special_codes','SpecialCodes')

class ReferenceTerminals(Reference):
    def __init__(self):
        super().__init__('dt_terminals','Terminals')

        self.equipment_id = None
        self.doc_tasks_id = None
        self.doc_works_id = None
        self.last_seen = None

    def find(self, id):
        res = super().find(id)

        if res:
            self.equipment_id = res[0].dt_equipments_id
            self.doc_works_id = res[0].dt_doc_works_id
            self.doc_tasks_id = res[0].dt_doc_tasks_id
            self.last_seen = res[0].last_seen
        else:
            self.equipment_id = None
            self.doc_works_id = None
            self.doc_tasks_id = None
            self.last_seen = None

        return res

class ReferenceEquipments(Reference):
    def __init__(self):
        super().__init__('dt_equipments','Equipments')


    def find(self, id):
        res = super().find(id)

        if res:
            self.line_id = res[0].line_id
        else:
            self.line_id = None

        return res


class Document(MetaData):
    def __init__(self, table_name, meta_ident):
        super().__init__(table_name,'Document',meta_ident)

        self.doc_number = ''

class DocumentTasks(Document):
    def __init__(self):
        super().__init__('dt_doc_tasks','Tasks')

    def find(self, id):
        res = super().find(id)

        if res:
            self.id = res[0].id
            self.doc_number = res[0].doc_number
            self.line_id = res[0].line_id
        else:
            self.id = None
            self.doc_number = ''
            self.line_id = None

        return res
