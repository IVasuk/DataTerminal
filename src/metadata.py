#!/usr/bin/env python3
from dt_dbms import Dbms

class MetaData:
    def __init__(self,table_name,meta_type,meta_ident):
        self.table_name = table_name
        self.meta_type = meta_type
        self.meta_ident = meta_ident

    def find(self,id):
        return Dbms.find(self.table_name,id)

    @staticmethod
    def connect():
        return Dbms.connect()

    @staticmethod
    def disconnect():
        return Dbms.disconnect()

class Reference(MetaData):
    def __init__(self, table_name, meta_ident):
        super().__init__(table_name,'Reference',meta_ident)

class ReferenceOperators(Reference):
    def __init__(self):
        super().__init__('dt_operators','Operators')

        self.id = ''
        self.name = ''

    def find(self,id):
        res = super().find(id)

        if res:
            self.id = res[0].id
            self.name = res[0].name
        else:
            self.id = ''
            self.name = ''

        return res

class ReferenceSpecialCodes(Reference):
    def __init__(self):
        super().__init__('dt_special_codes','SpecialCodes')

class ReferenceTerminals(Reference):
    def __init__(self):
        super().__init__('dt_terminals','Terminals')