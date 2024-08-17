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

class Handler:
    def on_destroy(self, *args):
        Gtk.main_quit()


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
    sc = label.get_style_context()

    css_str = ""

    if sc.has_class('green'):
        css_str = """
         #label_time {
            background: #FF0000;
            font-family: 'Arial 10';
            font-weight: normal;
            font-size: 35px;
        }"""

        sc.remove_class('green')
        sc.add_class('red')
    else:
        css_str = """
         #label_time {
             background: #008000;
             font-family: 'Arial 10';
             font-weight: normal;
             font-size: 35px;
         }"""

        sc.remove_class('red')
        sc.add_class('green')

    css_provider = Gtk.CssProvider()
    css_provider.load_from_data(bytes(css_str,'utf-8'))

    sc.add_provider(css_provider, Gtk.STYLE_PROVIDER_PRIORITY_USER)
    sc.remove_class('red')

    try:
        label.set_text(time.strftime("%H:%M:%S"))
    except:
        Gtk.main_quit()


def main():
    parser = create_parser()
    namespace = parser.parse_args()

    builder = Gtk.Builder()
    builder.add_from_file(os.path.join(CURRDIR, 'ui', 'main-window.glade'))
    builder.connect_signals(Handler())

    css_str = """
    #label_document_number {
        background: #000000;
        color: #FFFFFF;
        font-family: 'Arial 10';
        font-weight: normal;
        font-size: 25px;
    }
    #label_sername {
        background: #000000;
        color: #FFFFFF;
        font-family: 'Arial 10';
        font-weight: normal;
        font-size: 25px;
    }
    #label_info {
        background: #000000;
        color: #FFFFFF;
        font-family: 'Arial 10';
        font-weight: normal;
        font-size: 25px;
    }
    #label_time {
        font-family: 'Arial 10';
        font-weight: normal;
        font-size: 35px;
    }"""

    global css_provider

    css_provider = Gtk.CssProvider()
    css_provider.load_from_data(bytes(css_str,'utf-8'))

    style_context = Gtk.StyleContext()
    style_context.add_provider_for_screen(Gdk.Screen.get_default(), css_provider, Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION)
    window = builder.get_object('window_main')
#    window.maximize()
    window.show_all()

    try:
        pg_conn = psycopg2.connect(dbname=namespace.database, user=namespace.user, password=namespace.password, host=namespace.adress, port=namespace.port)
        pg_conn.autocommit = True
        pg_cursor = pg_conn.cursor()
    except:
        print("""Неможливо підключитися до бази даних:
                adress:%s
                port:%s
                database:%s
                user:%s
                password:%s"""%(namespace.adress, namespace.port, namespace.database, namespace.user, namespace.password))
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

