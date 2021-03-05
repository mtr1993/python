#coding=utf-8

import os,uuid
from docx import *
from docxtpl import DocxTemplate, InlineImage

class Chapter(object):
    def __init__(self, templ_docx):
        self._templ = DocxTemplate(templ_docx)
        self._uuid = str(uuid.uuid1()).replace("-", "")
        self._context = {}
        self._pic_to_add = {}

    def uuid(self):
        return self._uuid

    def add_context(self, context = {}):
        self._context = context.copy()

    def add_picture(self, name, path, width = 147):
        pic_name = "%s_%s" % (name, self._uuid)
        self._context[name] = "{{" + pic_name + "}}"
        self._pic_to_add[pic_name] = {"path": path, "width": width}

    def render(self, to_save_file = None):
        self._templ.render(self._context)

        if to_save_file is None:
            self.save(self.uuid() + ".docx")
        else:
            self.save(to_save_file)

    def save(self, filename = None):
        self._templ.save(filename)
        self._filename = filename

    def get_filename(self):
        return self._filename

    def get_docx(self):
        self._templ.get_docx()

    def remove_file(self):
        os.remove(self._filename)

    def get_pic_to_add(self):
        return self._pic_to_add

