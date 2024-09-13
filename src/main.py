#!/usr/bin/env python3
import datetime
import os
from os import terminal_size
from threading import Thread

import gi
import threading
import time
import argparse

gi.require_version('Gtk', '3.0')
gi.require_version('Notify', '0.7')

from gi.repository import Gtk
from gi.repository import Gdk
from gi.repository import GLib

from src.metadata import MetaData, ReferenceOperators, ReferenceSpecialCodes, DocumentTasks, ReferenceTerminals, \
    ReferenceEquipments, DocumentWorks, DocumentWorksItemsOperators, DocumentWorksItemsIntervals

CURRDIR = os.path.dirname(os.path.abspath(__file__))

class BarCodeObservable():
    def __init__(self, label, label_time):
        self.label = label
        self.label_time = label_time
        self.status = False
        self.barcode = []
        self.observers = []
        self.reading_status = False

    def set_label(self, label):
        self.label = label

    def set_label_text(self):
        if self.label:
            label_text = ''
            match DATAMODEL.status:
                case -3:
                    label_text = 'Перевірте структуру бази даних'
                case -2:
                    label_text = 'Перевірте довідник терміналів'
                case -1:
                    label_text = 'Скануйте код обладнання'
                case 0:
                    label_text = 'Скануйте свій код'
                case 1:
                    label_text = 'Для початку операції скануйте код документа'
                case 2:
                    label_text = 'Для завершення операції скануйте код документа'
                case 3:
                    label_text = 'Для продовження операції скануйте спеціальний код'

            self.label.set_text(label_text)
    #
    def set_label_time_background(self):
        sc = (self.label_time.get_style_context())

        css_str = ""

        if DATAMODEL.status == 2:
            css_str = """
                #label_time {
                background: #008000;
            }"""

            if sc.has_class('black'):
                sc.remove_class('black')

            if sc.has_class('red'):
                sc.remove_class('red')

            sc.add_class('green')
        elif DATAMODEL.status in (-3,-2,-1,3):
            css_str = """
             #label_time {
                 background: #FF0000;
             }"""

            if sc.has_class('black'):
                sc.remove_class('black')

            if sc.has_class('green'):
                sc.remove_class('green')

            sc.add_class('green')
        else:
            css_str = """
             #label_time {
                 background: #000000;
             }"""

            if sc.has_class('red'):
                sc.remove_class('red')

            if sc.has_class('green'):
                sc.remove_class('green')

            sc.add_class('black')

        css_provider = Gtk.CssProvider()
        css_provider.load_from_data(bytes(css_str, 'utf-8'))

        sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)

    def attach(self, observer):
        self.observers.append(observer)

    def detach(self, observer):
        self.observers.remove(observer)

    def add_char(self, value):
        sc = (self.label.get_style_context())

        if sc.has_class('red'):
            css_str = """
                #""" + Gtk.Buildable.get_name(self.label) + """ {
                background: #000000;
            }"""

            sc.remove_class('red')

            sc.add_class('black')

            css_provider = Gtk.CssProvider()
            css_provider.load_from_data(bytes(css_str, 'utf-8'))

            sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)

        self.reading_status = True
        self.barcode.append(value)

        self.label.set_text(''.join(self.barcode))

    def enter_barcode(self):
        res = False

        if len(self.barcode) != 0:
            buf = f"{int(''.join(self.barcode)):032x}"

            if len(self.barcode) == 4:
                buf = f"{int('140252029910160719407615654273820222181'):032x}"
            elif len(self.barcode) == 5:
                buf = f"{int('242526729503968700121204891150077040947'):032x}"
            elif len(self.barcode) == 6:
                buf = f"{int('14855292090638249075626786961853673841'):032x}"
            elif len(self.barcode) == 7:
                buf = f"{int('267493451463646028278735803215706623899'):032x}"

            id = f'{buf[:8]}-{buf[8:12]}-{buf[12:16]}-{buf[16:20]}-{buf[20:]}'

            for observer in self.observers:
                if observer.update(id):
                    res = True

                    break

        if not res:
            sc = (self.label.get_style_context())
            
            if not sc.has_class('red'):
                css_str = """
                    #""" + Gtk.Buildable.get_name(self.label) + """ {
                    background: #FF0000;
                }"""

                if sc.has_class('black'):
                    sc.remove_class('black')

                sc.add_class('red')

                css_provider = Gtk.CssProvider()
                css_provider.load_from_data(bytes(css_str, 'utf-8'))

                sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)

        for observer in self.observers:
            observer.set_label_text()

        self.set_label_time_background()

        self.reading_status = False
        self.barcode.clear()

        self.set_label_text()


class BarCodeObserver:
    def __init__(self, label=None):
        self.label = label

    def update(self, barcode):
        pass

    def set_label_text(self):
        pass


class OperatorsObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_operator(barcode)

        return res

    def set_label_text(self):
        self.label.set_text(DATAMODEL.str_operators())


class DocumentObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_document(barcode)

        return res

    def set_label_text(self):
        self.label.set_text(DATAMODEL.str_document())


class SpecialCodeObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_specialcode(barcode)

        return res

class EquipmentsObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_equipments_id(barcode)

        return res


class DataModel:
    def __init__(self):
        self.document_id = None
        self.document_number = ''
        self.operators = {}
        self.specialcode_id = None
        self.start_time = time.time()
        self.status = -3
        self.work_id = None
        self.current_interval = None
        self.terminal_id = None
        self.equipments_id = None

    def set_starttime(self):
        self.start_time = time.time()

    def get_starttime(self):
        return self.start_time

    def is_operators(self):
        return len(self.operators) > 0

    def operator_present(self, id):
        return id in self.operators

    def add_operator(self, id):
        res = False

        ref_operators = ReferenceOperators()

        if ref_operators.find(id):
            self.operators[str(ref_operators.id)] = ref_operators.name

            res = True
        else:
            res = False

        return res

    def del_operator(self, id):
        del self.operators[id]

        return True

    def clear_operators(self):
        self.operators.clear()

        return True

    def set_operator(self, id):
        res = False

        if self.status in (0, 1):
            if self.operator_present(id):
                res = self.del_operator(id)
            else:
                res = self.add_operator(id)

        self.set_status()

        return res

    def str_operators(self):
        return str('\n'.join(self.operators.values()))

    def is_document(self):
        return self.document_id is not None

    def document_present(self, id):
        return id == str(self.document_id)

    def add_document(self, id):
        res = False

        doc_tasks = DocumentTasks()

        if doc_tasks.find(id):
            if doc_tasks.line_id and not doc_tasks.status:
                ref_terminals = ReferenceTerminals()

                if ref_terminals.find(self.terminal_id):
                    ref_equipments = ReferenceEquipments()

                    if ref_equipments.find(ref_terminals.equipment_id):
                        if doc_tasks.line_id == ref_equipments.line_id:
                            res = True

                            doc_works = DocumentWorks()

                            docs = doc_works.find_by_requisite('task_id', doc_tasks.id)

                            if docs:
                                for doc in docs:
                                    if doc.status == 'complete':
                                        res = False

                                        break

                            if res:
                                self.document_id = doc_tasks.id
                                self.document_number = doc_tasks.doc_number

        return res

    def del_document(self):
        self.document_id = None
        self.document_number = ''

        return True

    def set_document(self, id):
        res = False

        if self.status == 2 or self.status == 3:
            if self.document_present(id):
                res = self.del_document() and self.clear_operators()

                if self.is_doc_works():
                    if self.status == 2:
                        self.set_doc_works_status('complete')
                    else:
                        self.set_doc_works_status('not_finished')

                    self.del_doc_works()
        elif self.status == 1:
            if self.document_present(id):
                self.del_doc_works()

                res = self.del_document()
            else:
                res = self.add_document(id)

                if res:
                    self.add_doc_works()

        self.set_status()

        return res

    def str_document(self):
        return str(self.document_number)

    def is_specialcode(self):
        return self.specialcode_id is not None

    def specialcode_present(self, id):
        return id == str(self.specialcode_id)

    def del_specialcode(self):
        self.specialcode_id = None

        return True

    def add_specialcode(self, id):
        res = False

        ref_specialcodes = ReferenceSpecialCodes()

        if ref_specialcodes.find(id):
            self.specialcode_id = ref_specialcodes.id

            res = True
        else:
            res = False

        return res

    def set_specialcode(self, id):
        res = False

        if self.status == 2:
            if self.specialcode_present(id):
                self.set_doc_works_status('work')

                res = self.del_specialcode()
            else:
                self.set_doc_works_status('paused')

                res = self.add_specialcode(id)
        elif self.status == 3:
            if self.specialcode_present(id):
                self.set_doc_works_status('work')

                res = self.del_specialcode()

        self.set_status()

        return res

    def add_equipments_id(self, id):
        res = False

        ref_equipments = ReferenceEquipments()

        if ref_equipments.find(id):
            self.equipments_id = ref_equipments.id

            res = True
        else:
            res = False

        return res

    def is_equipments_id(self):
        return self.equipments_id is not None

    def equipments_id_present(self, id):
        return id == str(self.equipments_id)

    def set_equipments_id(self, id):
        res = False

        if (self.status in (-1, 0)) and self.is_terminal_id():
            if self.equipments_id_present(id):
                res = True
            elif self.add_equipments_id(id):
                ref_terminals = ReferenceTerminals()

                if ref_terminals.find(self.terminal_id):
                    ref_terminals.equipment_id = self.equipments_id

                    if ref_terminals.save():
                        res = True
                else:
                    self.del_equipments_id()

        self.set_status()

        return res

    def add_terminal_id(self, id):
        res = False

        ref_terminals = ReferenceTerminals()

        if ref_terminals.find(id):
            self.terminal_id = ref_terminals.id

            res = True
        else:
            res = False

        return res

    def is_terminal_id(self):
        return self.terminal_id is not None

    def terminal_id_present(self, id):
        return id == str(self.terminal_id)

    def set_terminal_id(self, id):
        res = False

        if self.status in (-3, -2, -1, 0):
            ref_terminals = ReferenceTerminals()

            if ref_terminals.find(id):
                self.add_terminal_id(ref_terminals.id)

        self.set_status()

        return res

    def is_doc_works(self):
        return self.work_id is not None

    def add_doc_works(self):
        res = False

        doc_works = DocumentWorks.new_document()
        doc_works.task_id = self.document_id
        doc_works.equipment_id = self.equipments_id
        doc_works.status = 'work'

        res = doc_works.save()

        if res is not False:
            self.work_id = doc_works.id

            for operator in self.operators:
                item = doc_works.add_item(DocumentWorksItemsOperators.META_IDENT)

                item.operator_id = operator

                if item.save() is False:
                    res = False

                    break

            if res:
                item = doc_works.add_item(DocumentWorksItemsIntervals.META_IDENT)
                item.begin = datetime.datetime.now()

                if item.save() is False:
                    self.current_interval = None

                    res = False
                else:
                    self.current_interval = item.position
        return res

    def del_doc_works(self):
        self.work_id = None
        self.current_interval = None

        return True

    def set_doc_works_status(self,status):
        res = False

        if self.work_id:
            doc_work = DocumentWorks()

            if doc_work.find(self.work_id):
                if self.current_interval:
                    items = doc_work.get_item(DocumentWorksItemsIntervals.META_IDENT,self.current_interval)

                    if items:
                        items.end = datetime.datetime.now(tz=items.begin.tzinfo)

                        if doc_work.status == 'work':
                            items.work_interval = int((items.end - items.begin).total_seconds())
                        elif doc_work.status == 'paused':
                            items.stop_interval = int((items.end - items.begin).total_seconds())

                        if items.save():
                            res = True

                if res and (status in ('work','paused')):
                    items = doc_work.add_item(DocumentWorksItemsIntervals.META_IDENT)
                    items.begin = datetime.datetime.now()

                    if items.save():
                        self.current_interval = items.position

                        res = True

                if res:
                    doc_work.status = status

                    if doc_work.save():
                        res = True
            else:
                self.del_doc_works()

        return res

    def set_status(self):
        if self.status == 0:
            if self.is_operators():
                self.status = 1
        elif self.status == 1:
            if self.is_document():
                self.status = 2

                self.set_starttime()
            elif not self.is_operators():
                self.status = 0
        elif self.status == 2:
            if not self.is_operators():
                self.status = 0
            elif not self.is_document():
                self.status = 1
            elif self.is_specialcode():
                self.status = 3

                self.set_starttime()
        elif self.status == 3:
            if not self.is_operators():
                self.status = 0
            elif not self.is_document():
                self.status = 1
            elif not self.is_specialcode():
                self.status = 2

                self.set_starttime()
        elif self.status == -1:
            if self.is_equipments_id():
                self.status = 0
        elif self.status == -2:
            if self.is_terminal_id():
                self.status = -1
        elif self.status == -3:
            if MetaData.get_terminal_id():
                self.status = -2

        return self.status


class Handler:
    def on_destroy(self, *args):
        Gtk.main_quit()

    def on_button_release_event(self, *args):
        Gtk.main_quit()

    def on_key_release_event(self, *args):
        ch = chr(args[1].keyval)
        if ch.isdigit():
            BARCODEPUBLISHER.add_char(ch)
        elif args[1].keyval == Gdk.KEY_Tab:
            BARCODEPUBLISHER.enter_barcode()


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.next_call = time.time()
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        GLib.idle_add(self.function, *self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self.next_call += self.interval
            self._timer = threading.Timer(self.next_call - time.time(), self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--adress', required=False, type=str, default='localhost')
    parser.add_argument('-p', '--port', required=False, type=str, default='5432')
    parser.add_argument('-d', '--dbname', required=False, type=str, default='dataterminal')
    parser.add_argument('-u', '--user', required=False, type=str, default='dataterminal')
    parser.add_argument('-pas', '--password', required=False, type=str, default='terminal')

    return parser


def update_indicator(label,dbms=None):
    try:
        if DATAMODEL.status == 2 or DATAMODEL.status == 3:
            hours, remaining = divmod(int(time.time() - DATAMODEL.get_starttime()), 3600)
            minutes, seconds = divmod(remaining, 60)

            label.set_text(f"{hours:02}:{minutes:02}:{seconds:02}")
        else:
            label.set_text(time.strftime("%H:%M:%S"))

        if dbms.connected() and DATAMODEL.is_terminal_id():
            ref_terminals = ReferenceTerminals()

            if ref_terminals.find(DATAMODEL.terminal_id):
                ref_terminals.doc_tasks_id = DATAMODEL.document_id
                ref_terminals.doc_works_id = DATAMODEL.work_id
                ref_terminals.last_seen = datetime.datetime.now()
                ref_terminals.save()

            if not DATAMODEL.current_interval is None:
                RT_MUTEX.acquire()

                doc_works = DocumentWorks()

                if doc_works.find(DATAMODEL.work_id):
                    items = doc_works.get_item(DocumentWorksItemsIntervals.META_IDENT,DATAMODEL.current_interval)

                    if items:
                        items.end = datetime.datetime.now(tz=items.begin.tzinfo)

                        if doc_works.status == 'work':
                            items.work_interval = int((items.end - items.begin).total_seconds())
                        elif doc_works.status == 'paused':
                            items.stop_interval = int((items.end - items.begin).total_seconds())

                        items.save()

                RT_MUTEX.release()
    except:
        Gtk.main_quit()


def main():
    parser = create_parser()
    namespace = parser.parse_args()

    global DATAMODEL

    DATAMODEL = DataModel()

    builder = Gtk.Builder()
    builder.add_from_file(os.path.join(CURRDIR, 'ui', 'main-window.glade'))
    builder.connect_signals(Handler())

    global BARCODEPUBLISHER

    BARCODEPUBLISHER = BarCodeObservable(builder.get_object('label_info'), builder.get_object('label_time'))

    operator_observer = OperatorsObserver(builder.get_object('label_sername'))
    document_observer = DocumentObserver(builder.get_object('label_document_number'))
    specialcode_observer = SpecialCodeObserver(builder.get_object('label_time'))
    equipments_observer = EquipmentsObserver(builder.get_object('label_document_number'))

    BARCODEPUBLISHER.attach(operator_observer)
    BARCODEPUBLISHER.attach(document_observer)
    BARCODEPUBLISHER.attach(specialcode_observer)
    BARCODEPUBLISHER.attach(equipments_observer)

    css_str = """
    #label_document_number {
        background: #000000;
        color: #FFFFFF;
    }
    #label_sername {
        background: #000000;
        color: #FFFFFF;
    }
    #label_info {
        background: #000000;
        color: #FFFFFF;
    }
    #label_time {
        background: #000000;
        color: #FFFFFF;
    }"""

    css_provider = Gtk.CssProvider()
    css_provider.load_from_data(bytes(css_str, 'utf-8'))

    style_context = Gtk.StyleContext()
    style_context.add_provider_for_screen(Gdk.Screen.get_default(), css_provider,
                                          Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION)

    window = builder.get_object('window_main')
    window.fullscreen()
    window.maximize()
    window.show_all()

    dbms_thread = MetaData.get_new_dbms(adress=namespace.adress, port=namespace.port, dbname=namespace.dbname, user=namespace.user, password=namespace.password)

    rt = RepeatedTimer(1, update_indicator, builder.get_object('label_time'), dbms_thread)  # it auto-starts, no need of rt.start()

    global RT_MUTEX

    RT_MUTEX = threading.Lock()

    MetaData.set_connection_params(adress=namespace.adress, port=namespace.port, dbname=namespace.dbname, user=namespace.user,
                          password=namespace.password)

    try:
        MetaData.connect()

        if MetaData.set_terminal_id():
            DATAMODEL.set_status()

            if DATAMODEL.add_terminal_id(MetaData.get_terminal_id()):
                DATAMODEL.set_status()

                ref_terminals = ReferenceTerminals()
                if ref_terminals.find(DATAMODEL.terminal_id):
                    DATAMODEL.add_equipments_id(ref_terminals.equipment_id)

                    DATAMODEL.set_status()

        BARCODEPUBLISHER.set_label_text()
        BARCODEPUBLISHER.set_label_time_background()

        dbms_thread.connect()

        Gtk.main()
    finally:
        rt.stop()  # better in a try/finally block to make sure the program ends!

        MetaData.disconnect()

        dbms_thread.disconnect()

if __name__ == '__main__':
    main()
