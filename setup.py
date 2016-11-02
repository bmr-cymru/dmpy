#!/usr/bin/env python

from distutils.core import setup, Extension

dmpy_module = Extension('dmpy',
                        libraries=['devmapper'],
                        sources=['dmpy/dmpymodule.c'])

setup(name='dmpy',
      version="0.1",
      description=("""Python bindings for device-mapper."""),
      author='Bryn M. Reeves',
      author_email='bmr@redhat.com',
      url='https://github.com/bmr-cymru/dmpy',
      license="GPLv2",
      #packages=['dmpy'],
      ext_modules=[dmpy_module]
     )


# vim: set et ts=4 sw=4 :
