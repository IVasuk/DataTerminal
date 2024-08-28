#!/usr/bin/env python3

import os
import gi
import threading
import psycopg2
import time
import sys
import argparse

gi.require_version('Gtk', '3.0')
gi.require_version('Notify', '0.7')

from gi.repository import Gtk
from gi.repository import Gdk

CURRDIR = os.path.dirname(os.path.abspath(__file__))


class BarCodeObservable():
    def __init__(self, label=None):
        self.label = label
        self.status = False
        self.barcode = []
        self.observers = []

    def set_label(self, label):
        self.label = label

    def attach(self, observer):
        self.observers.append(observer)

    def detach(self, observer):
        self.remove(observer)

    def add_char(self, value):
        sc = (self.label.get_style_context())

        css_str = ""

        if sc.has_class('red'):
            css_str = """
                #label_info {
                background: #000000;
            }"""

            sc.remove_class('red')

        sc.add_class('black')

        css_provider = Gtk.CssProvider()
        css_provider.load_from_data(bytes(css_str, 'utf-8'))

        sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)

        self.reading_status = True
        self.barcode.append(value)

        if self.label:
            self.label.set_text(''.join(self.barcode))

    def enter_barcode(self):
        res = False

        for observer in self.observers:
            if observer.update(''.join(self.barcode)):
                res = True

                break

        if not res:
            sc = (self.label.get_style_context())

            css_str = ""

            if sc.has_class('black'):
                css_str = """
                    #label_info {
                    background: #FF0000;
                }"""

                sc.remove_class('green')

            sc.add_class('red')

            css_provider = Gtk.CssProvider()
            css_provider.load_from_data(bytes(css_str, 'utf-8'))

            sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)

        self.reading_status = False
        self.barcode.clear()

        if self.label:
            label_text = ''
            match DATAMODEL.status:
                case 0:
                    label_text = 'Скануйте свій код'
                case 1:
                    label_text = 'Для початку операції скануйте код документа'
                case 2:
                    label_text = 'Для завершення операції скануйте код документа'
                case 3:
                    label_text = 'Для продовження операції скануйте спеціальний код'

            self.label.set_text(label_text)


class BarCodeObserver():
    def __init__(self, label=None):
        self.label = label

    def update(self, barcode):
        pass


class OperatorsObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_operator(barcode)

        self.label.set_text(DATAMODEL.str_operators())

        return res


class DocumentObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_document(barcode)

        self.label.set_text(DATAMODEL.str_document())

        return res

class SpecialCodeObserver(BarCodeObserver):
    def update(self, barcode):
        res = DATAMODEL.set_specialcode(barcode)

        sc = (self.label.get_style_context())

        css_str = ""

        if DATAMODEL.is_specialcode():
            if sc.has_class('green'):
                css_str = """
                    #label_time {
                    background: #FF0000;
                }"""

            sc.remove_class('green')
            sc.add_class('red')
        else:
            css_str = """
             #label_time {
                 background: #008000;
             }"""

            sc.remove_class('red')
            sc.add_class('green')

        css_provider = Gtk.CssProvider()
        css_provider.load_from_data(bytes(css_str, 'utf-8'))

        sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)

        return res

class DataModel:
    def __init__(self):
        self.document_barcode = ''
        self.document_number = ''
        self.operators = {}
        self.specialcode = False
        self.start_time = time.time()
        self.status = 0

    def set_starttime(self):
        self.start_time = time.time()

    def get_starttime(self):
        return self.start_time

    def is_operators(self):
        return len(self.operators) > 0

    def operator_present(self, barcode):
        return barcode in self.operators

    def add_operator(self, barcode):
        self.operators[barcode] = 'Name: ' + barcode

    def del_operator(self, barcode):
        del self.operators[barcode]

    def set_operator(self, barcode):
        if len(barcode) != 5:
            return False

        res = False

        if self.status < 2:
            if self.operator_present(barcode):
                self.del_operator(barcode)
            else:
                self.add_operator(barcode)

            self.set_status()

            res = True

        return res

    def str_operators(self):
        return str('\n'.join(self.operators.values()))

    def is_document(self):
        return len(self.document_barcode) > 0

    def document_present(self, barcode):
        return barcode == self.document_barcode

    def add_document(self, barcode):
        self.document_barcode = barcode
        self.document_number = f"Number: {barcode}"

    def del_document(self):
        self.document_barcode = ''
        self.document_number = ''


    def set_document(self, barcode):
        if len(barcode) != 4:
            return False

        res = False

        if self.status == 2:
            if self.document_present(barcode):
                self.del_document()

                res = True
        elif self.status == 1:
            if self.document_present(barcode):
                self.del_document()
            else:
                self.add_document(barcode)

            res = True

        self.set_status()

        return res

    def str_document(self):
        return str(self.document_number)

    def is_specialcode(self):
        return self.specialcode

    def set_specialcode(self,barcode):
        if len(barcode) != 6:
            return False

        res = False

        if self.status == 2 or self.status == 3:
            self.specialcode = not self.specialcode

            res = True

            self.set_status()

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
            if not self.is_document():
                self.status = 1
            elif self.is_specialcode():
                self.status = 3

                self.set_starttime()
        elif self.status == 3:
            if not self.is_specialcode():
                self.status = 2

                self.set_starttime()

        return self.status


class Handler:
    def on_destroy(self, *args):
        Gtk.main_quit()

    def on_key_release_event(self, *args):
        ch = chr(args[1].keyval)

        if ch.isalnum():
            BARCODEPUBLISHER.add_char(ch)
        else:
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
        self.function(*self.args, **self.kwargs)

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
    parser.add_argument('-d', '--database', required=False, type=str, default='dataterminal')
    parser.add_argument('-u', '--user', required=False, type=str, default='dataterminal')
    parser.add_argument('-pas', '--password', required=False, type=str, default='terminal')

    return parser


def update_indicator(label):
    try:
        if DATAMODEL.status == 2 or DATAMODEL.status == 3:
            hours, remaining = divmod(int(time.time() - DATAMODEL.get_starttime()), 3600)
            minutes, seconds = divmod(remaining, 60)

            label.set_text(f"{hours:02}:{minutes:02}:{seconds:02}")
        else:
            label.set_text("00:00:00")
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

    BARCODEPUBLISHER = BarCodeObservable(builder.get_object('label_info'))

    operator_observer = OperatorsObserver(builder.get_object('label_sername'))
    document_observer = DocumentObserver(builder.get_object('label_document_number'))
    specialcode_observer = SpecialCodeObserver(builder.get_object('label_time'))

    BARCODEPUBLISHER.attach(operator_observer)
    BARCODEPUBLISHER.attach(document_observer)
    BARCODEPUBLISHER.attach(specialcode_observer)

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
        background: #008000;
        color: #FFFFFF;
    }"""

    global css_provider

    css_provider = Gtk.CssProvider()
    css_provider.load_from_data(bytes(css_str, 'utf-8'))

    style_context = Gtk.StyleContext()
    style_context.add_provider_for_screen(Gdk.Screen.get_default(), css_provider,
                                          Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION)

    window = builder.get_object('window_main')
    window.maximize()
    window.show_all()

    try:
        pg_conn = psycopg2.connect(dbname=namespace.database, user=namespace.user, password=namespace.password,
                                   host=namespace.adress, port=namespace.port)
        pg_conn.autocommit = True
        pg_cursor = pg_conn.cursor()
    except:
        print("""Неможливо підключитися до бази даних:
                adress:%s
                port:%s
                database:%s
                user:%s
                password:%s""" % (
        namespace.adress, namespace.port, namespace.database, namespace.user, namespace.password))
        exit()

    rt = RepeatedTimer(1, update_indicator, builder.get_object('label_time'))  # it auto-starts, no need of rt.start()

    try:
        Gtk.main()
    finally:
        rt.stop()  # better in a try/finally block to make sure the program ends!
        pg_cursor.close()
        pg_conn.close()


if __name__ == '__main__':
    main()
