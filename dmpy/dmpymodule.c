/*
 * Copyright (C) 2016 Red Hat, Inc., Bryn M. Reeves <bmr@redhat.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2, as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 *
 * dmpy - Python device-mapper interface.
 *
 */

#include "Python.h"
#include "structmember.h"
#include "libdevmapper.h"
#include "sys/types.h"
#include "stdint.h"
#include "string.h"

/* DM_{NAME,UUID}_LEN */
#include <linux/dm-ioctl.h>

#define DMPY_VERSION_BUF_LEN 64

/* The MAJOR and MINOR macros exported in uapi/linux/kdev_t.h do not
 * work with "huge" device numbers (>8bit minor, for e.g.). Use the
 * definitions found in lvm2/libdm/misc/kdev_t.h instead.
 */
#define MAJOR(dev)      ((dev & 0xfff00) >> 8)
#define MINOR(dev)      ((dev & 0xff) | ((dev >> 12) & 0xfff00))
#define MKDEV(ma,mi)    ((mi & 0xff) | (ma << 8) | ((mi & ~0xff) << 12))

#define DMPY_DEBUG 1

#ifdef DMPY_DEBUG
    #define dmpy_debug(x...) fprintf(stderr, x)
#else
    #define dmpy_debug(x...)
#endif

#define NSEC_PER_USEC   1000L
#define NSEC_PER_MSEC   1000000L
#define NSEC_PER_SEC    1000000000L

PyDoc_STRVAR(dmpy__doc__,
"dmpy is a set of Python bindings for the device-mapper library.\n");

/* order must match libdevmapper.h task type enum */
static const char *_dm_task_type_names[] = {
    "DM_DEVICE_CREATE",
    "DM_DEVICE_RELOAD",
    "DM_DEVICE_REMOVE",
    "DM_DEVICE_REMOVE_ALL",
    "DM_DEVICE_SUSPEND",
    "DM_DEVICE_RESUME",
    "DM_DEVICE_INFO",
    "DM_DEVICE_DEPS",
    "DM_DEVICE_RENAME",
    "DM_DEVICE_VERSION",
    "DM_DEVICE_STATUS",
    "DM_DEVICE_TABLE",
    "DM_DEVICE_WAITEVENT",
    "DM_DEVICE_LIST",
    "DM_DEVICE_CLEAR",
    "DM_DEVICE_MKNODES",
    "DM_DEVICE_LIST_VERSIONS",
    "DM_DEVICE_TARGET_MSG",
    "DM_DEVICE_SET_GEOMETRY"
};

/* Dm objects */

static PyObject *DmErrorObject;


typedef struct {
    PyObject_HEAD
    struct dm_timestamp *ob_ts;
} DmTimestampObject;

static PyTypeObject DmTimestamp_Type;

#define DmTimestampObject_Check(v)      (Py_TYPE(v) == &DmTimestamp_Type)

static DmTimestampObject *
newDmTimestampObject(void)
{
    return PyObject_New(DmTimestampObject, &DmTimestamp_Type);
}

static int
DmTimestamp_init(DmTimestampObject *self, PyObject *args, PyObject *kwds)
{
    if (!PyArg_ParseTuple(args, ":__init__"))
        return -1;

    self->ob_ts = dm_timestamp_alloc();
    if (!self->ob_ts) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate dm_timestamp.");
        return -1;
    }
    return 0;
}

static void
DmTimestamp_dealloc(DmTimestampObject *self)
{
    if (self->ob_ts)
        dm_timestamp_destroy(self->ob_ts);
    self->ob_ts = NULL;

    PyObject_Del(self);
}

/* DmTimestamp methods */

static PyObject *
DmTimestamp_copy(DmTimestampObject *self, PyObject *args)
{
    DmTimestampObject *copy;

    if (!(copy = newDmTimestampObject()))
        return NULL;

    copy->ob_ts = dm_timestamp_alloc();
    if (!self->ob_ts) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate dm_timestamp.");
        return NULL;
    }
    dm_timestamp_copy(copy->ob_ts, self->ob_ts);

    return (PyObject *) copy;
}

static PyObject *
DmTimestamp_get(DmTimestampObject *self, PyObject *args)
{
    if (!dm_timestamp_get(self->ob_ts)) {
        PyErr_SetString(PyExc_OSError, "Failed to get device-mapper "
                        "timestamp.");
        return NULL;
    }
    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTimestamp_compare(DmTimestampObject *self, PyObject *args)
{
    DmTimestampObject *to;

    if (!PyArg_ParseTuple(args, "O!:compare", &DmTimestamp_Type, &to))
        return NULL;

    return Py_BuildValue("i", dm_timestamp_compare(self->ob_ts, to->ob_ts));
}

static PyObject *
DmTimestamp_delta(DmTimestampObject *self, PyObject *args)
{
    DmTimestampObject *to;

    if (!PyArg_ParseTuple(args, "O!:compare", &DmTimestamp_Type, &to))
        return NULL;

    return Py_BuildValue("i", dm_timestamp_delta(self->ob_ts, to->ob_ts));
}

#define DMTIMESTAMP_copy__doc__ \
"Copy this DmTimestamp."

#define DMTIMESTAMP_get__doc__ \
"Update this DmTimestamp to the current time."

#define DMTIMESTAMP_compare__doc__ \
"Compare two DmTimestamp objects."

#define DMTIMESTAMP_delta__doc__ \
"Return the absolute difference, in nanoseconds, between two " \
"DmTimestamp objects."

static PyMethodDef DmTimestamp_methods[] = {
    {"copy", (PyCFunction)DmTimestamp_copy, METH_VARARGS,
        PyDoc_STR(DMTIMESTAMP_copy__doc__)},
    {"get", (PyCFunction)DmTimestamp_get, METH_NOARGS,
        PyDoc_STR(DMTIMESTAMP_get__doc__)},
    {"compare", (PyCFunction)DmTimestamp_compare, METH_VARARGS,
        PyDoc_STR(DMTIMESTAMP_compare__doc__)},
    {"delta", (PyCFunction)DmTimestamp_delta, METH_VARARGS,
        PyDoc_STR(DMTIMESTAMP_delta__doc__)},
    {NULL, NULL}
};

static PyTypeObject DmTimestamp_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmTimestamp",             /*tp_name*/
    sizeof(DmTimestampObject),          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)DmTimestamp_dealloc, /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    DmTimestamp_methods,        /*tp_methods*/
    0,           /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmTimestamp_init,    /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};
 

typedef struct {
    PyObject_HEAD
    uint32_t ob_cookie;
    uint16_t ob_val_prefix;
    uint16_t ob_val_base;
    PyObject *ob_ready; /* Py_True / Py_False */
} DmCookieObject;

static PyTypeObject DmCookie_Type;

#define DmCookieObject_Check(v)      (Py_TYPE(v) == &DmCookie_Type)

static void
_dmpy_set_cookie_values(DmCookieObject *self)
{
    self->ob_val_base = self->ob_cookie & ~DM_UDEV_FLAGS_MASK;
    self->ob_val_prefix = ((self->ob_cookie & DM_UDEV_FLAGS_MASK)
                           >> DM_UDEV_FLAGS_SHIFT);
}

static int
_dmpy_check_cookie_value(unsigned value)
{
    if (value > 0xffffffff) {
        PyErr_SetString(PyExc_ValueError, "DmCookie value out of range.");
        return -1;
    }
    return 0;
}

static void
DmCookie_dealloc(DmCookieObject *self)
{
    /* ob_ready may be NULL if __init__ failed, or was never called. */
    Py_XDECREF(self->ob_ready);
    PyObject_Del(self);
}

static int
_DmCookie_init(DmCookieObject *self, uint32_t value)
{
    if (_dmpy_check_cookie_value((unsigned) value))
        return -1;

    self->ob_cookie = (uint32_t) value;
    _dmpy_set_cookie_values(self);

    Py_INCREF(Py_False);
    self->ob_ready = Py_False;

    return 0;
}

static int
DmCookie_init(DmCookieObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"value", NULL};
    int value = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:__init__",
                                     kwlist, &value))
        return -1;

    return _DmCookie_init(self, value);
}

static PyObject *
DmCookie_set_value(DmCookieObject *self, PyObject *args)
{
    int value;

    if (!PyArg_ParseTuple(args, "i:set_value", &value))
        return NULL;

    if (!_dmpy_check_cookie_value(value))
        return NULL;

    self->ob_cookie = (uint32_t) value;
    _dmpy_set_cookie_values(self);

    Py_INCREF(Py_True);
    return Py_True;
}

static int
_dmpy_check_base_or_prefix_value(unsigned value)
{
    if (value > 0xffff) {
        PyErr_SetString(PyExc_ValueError, "DmCookie prefix value out of range.");
        return -1;
    }
    return 0;
}

static PyObject *
DmCookie_set_prefix(DmCookieObject *self, PyObject *args)
{
    int prefix;

    if (!PyArg_ParseTuple(args, "i:set_prefix", &prefix))
        return NULL;

    if (_dmpy_check_base_or_prefix_value(prefix))
        return NULL;

    prefix <<= DM_UDEV_FLAGS_SHIFT;

    self->ob_cookie = (uint32_t) (self->ob_cookie & ~DM_UDEV_FLAGS_MASK);
    self->ob_cookie |= prefix;
    _dmpy_set_cookie_values(self);

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmCookie_set_base(DmCookieObject *self, PyObject *args)
{
    int base;
    if (!PyArg_ParseTuple(args, "i:set_base", &base))
        return NULL;

    if (_dmpy_check_base_or_prefix_value(base))
        return NULL;

    self->ob_cookie = (uint32_t) (self->ob_cookie & DM_UDEV_FLAGS_MASK);
    self->ob_cookie |= base;
    _dmpy_set_cookie_values(self);

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmCookie_udev_complete(DmCookieObject *self, PyObject *args)
{
    PyObject *ret;
    int r;
    r = dm_udev_complete(self->ob_cookie);
    ret = (r) ? Py_True : Py_False;
    Py_INCREF(ret);
    return ret;
}

static PyObject *
_DmCookie_udev_wait(DmCookieObject *self, int immediate)
{
    PyObject *ret;
    int r, ready;

    if (self->ob_ready == Py_True) {
        PyErr_SetString(PyExc_ValueError, "Cannot udev_wait() on a "
                        "completed DmCookie.");
        return NULL;
    }

    if (!immediate) {
        r = dm_udev_wait(self->ob_cookie);
        ready = r;
    } else
        r = dm_udev_wait_immediate(self->ob_cookie, &ready);

    ret = (r) ? Py_True : Py_False;
    Py_INCREF(ret);

    if (r && ready) {
        Py_DECREF(self->ob_ready);
        Py_INCREF(Py_True);
        self->ob_ready = Py_True;
    }

    return ret;
}

static PyObject *
DmCookie_udev_wait(DmCookieObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"immediate", NULL};
    int immediate = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:udev_wait",
                                     kwlist, &immediate))
        return NULL;

    return _DmCookie_udev_wait(self, immediate);
}

#define DMCOOKIE_set_value__doc__ \
"Set the value of this DmCookie to the given integer. The cookie is "   \
"stored internally as a 32-bit value by the kernel and device-mapper: " \
"attempting to store a larger value will raise a ValueError."

#define DMCOOKIE_set_prefix__doc__ \
"Set the prefix of this DmCookie to the given integer. The prefix is "   \
"stored internally as a 16-bit value by the kernel and device-mapper: " \
"attempting to store a larger prefix will raise a ValueError."

#define DMCOOKIE_set_base__doc__ \
"Set the base of this DmCookie to the given integer. The base is "   \
"stored internally as a 16-bit value by the kernel and device-mapper: " \
"attempting to store a larger base will raise a ValueError."

#define DMCOOKIE_udev_complete__doc__ \
"Complete the UDEV transaction for this DmCookie."

#define DMCOOKIE_udev_wait__doc__ \
"Wait until the transaction represented by this `DmCookie` is ready.\n\n"  \
"Returns `True` if the wait is successful and the transaction is\n"        \
"complete.\n\n"                                                            \
"Following a successful wait the `DmCookie.ready` member is set to\n"      \
"`True`.\n\n"                                                              \
"On error an OSError exception is raised.\n\n"                             \
"Unless `immediate=True` and the transaction is not ready, the cookie\n"   \
"resources are released and no further calls to `udev_wait()`,\n"          \
"or `udev_complete() should be made."

static PyMethodDef DmCookie_methods[] = {
    {"set_value", (PyCFunction)DmCookie_set_value, METH_VARARGS,
        PyDoc_STR(DMCOOKIE_set_value__doc__)},
    {"set_prefix", (PyCFunction)DmCookie_set_prefix, METH_VARARGS,
        PyDoc_STR(DMCOOKIE_set_prefix__doc__)},
    {"set_base", (PyCFunction)DmCookie_set_base, METH_VARARGS,
        PyDoc_STR(DMCOOKIE_set_base__doc__)},
    {"udev_complete", (PyCFunction)DmCookie_udev_complete, METH_NOARGS,
        PyDoc_STR(DMCOOKIE_udev_complete__doc__)},
    {"udev_wait", (PyCFunction)DmCookie_udev_wait,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(DMCOOKIE_udev_wait__doc__)},
    {NULL, NULL}
};

/* Attributes for DmCookieObject struct members.
 */
static PyMemberDef DmCookie_members[] = {
    {"value", T_INT, offsetof(DmCookieObject, ob_cookie), READONLY,
     PyDoc_STR("The current value of this `DmCookie`.")},
    {"prefix", T_SHORT, offsetof(DmCookieObject, ob_val_prefix), READONLY,
     PyDoc_STR("The current prefix value of this `DmCookie`.")},
    {"base", T_SHORT, offsetof(DmCookieObject, ob_val_base), READONLY,
     PyDoc_STR("The current base value of this `DmCookie`.")},
    {"ready", T_OBJECT, offsetof(DmCookieObject, ob_ready), READONLY,
     PyDoc_STR("The current completion state of this `DmCookie`.")},
    {NULL}
};

static PyTypeObject DmCookie_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmCookie",             /*tp_name*/
    sizeof(DmCookieObject),          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)DmCookie_dealloc, /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    DmCookie_methods,           /*tp_methods*/
    DmCookie_members,           /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmCookie_init,    /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};


typedef struct {
    PyObject_HEAD
    struct dm_info ob_info;
} DmInfoObject;

static PyTypeObject DmInfo_Type;

#define DmInfoObject_Check(v)      (Py_TYPE(v) == &DmInfo_Type)

static DmInfoObject *
newDmInfoObject(void)
{
    return PyObject_New(DmInfoObject, &DmInfo_Type);
}

static int
DmInfo_init(DmInfoObject *self, PyObject *args, PyObject *kwds)
{
    if (!PyArg_ParseTuple(args, ":__init__"))
        return -1;

    memset(&self->ob_info, 0, sizeof(self->ob_info));
    return 0;
}

/* Attributes for dm_info struct members.
 */
static PyMemberDef DmInfo_members[] = {
    {"exists", T_INT, offsetof(DmInfoObject, ob_info.exists), READONLY,
     PyDoc_STR("Flag indicating whether this device exists.")},
    {"suspended", T_INT, offsetof(DmInfoObject, ob_info.suspended), READONLY,
     PyDoc_STR("Flag indicating whether this device is suspended.")},
    {"live_table", T_INT, offsetof(DmInfoObject, ob_info.live_table), READONLY,
     PyDoc_STR("Flag indicating whether this device has a live table.")},
    {"inactive_table", T_INT, offsetof(DmInfoObject, ob_info.inactive_table),
     READONLY, PyDoc_STR("Flag indicating whether this device has an inactive"
     "table.")},
    {"open_count", T_INT, offsetof(DmInfoObject, ob_info.open_count), READONLY,
     PyDoc_STR("Count of open references to this device.")},
    {"event_nr", T_UINT, offsetof(DmInfoObject, ob_info.event_nr), READONLY,
     PyDoc_STR("The current event counter value for the device.")},
    {"major", T_UINT, offsetof(DmInfoObject, ob_info.major), READONLY,
     PyDoc_STR("The major number of the device.")},
    {"minor", T_UINT, offsetof(DmInfoObject, ob_info.minor), READONLY,
     PyDoc_STR("The minor number of the device.")},
    {"read_only", T_INT, offsetof(DmInfoObject, ob_info.read_only), READONLY,
     PyDoc_STR("The read-only state of the device (0=rw, 1=ro).")},
    {"target_count", T_INT, offsetof(DmInfoObject, ob_info.target_count),
     READONLY, PyDoc_STR("Number of targets in the live table.")},
    {"deferred_remove", T_INT, offsetof(DmInfoObject, ob_info.deferred_remove),
     READONLY, PyDoc_STR("Flag indicating whether deferred removal is enabled "
     "for the device.")},
    {"internal_suspend", T_INT, offsetof(DmInfoObject, ob_info.internal_suspend),
     READONLY, PyDoc_STR("Flag indicating whether the device is suspended"
     "internally by the device-mapper.")},
    {NULL}
};

static PyTypeObject DmInfo_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmInfo",             /*tp_name*/
    sizeof(DmInfoObject),          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    0,                          /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    0,                          /*tp_methods*/
    DmInfo_members,             /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmInfo_init,      /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};

/* dm_task state flags: since `struct dm_task` is not exposed in
 * `libdevmapper.h`, maintain flags indicating the validity of the various
 * members, following issue of a particular `DM_DEVICE_*` command.
 */
#define DMT_DID_IOCTL           0x00000001
#define DMT_DID_ERROR           0x00000002
#define DMT_HAVE_INFO           0x00000010
#define DMT_HAVE_NAME           0x00000020
#define DMT_HAVE_UUID           0x00000040
#define DMT_HAVE_DEPS           0x00000080
#define DMT_HAVE_NAME_LIST      0x00000100
#define DMT_HAVE_TIMESTAMP      0x00000200
#define DMT_HAVE_MESSAGE        0x00000400
#define DMT_HAVE_TABLE          0x00000800
#define DMT_HAVE_STATUS         0x00001000
#define DMT_HAVE_TARGET_VERSIONS 0x00002000

/* FIXME: maj/min? */
#define DMT_HAVE_IDENTITY (DMT_HAVE_NAME | DMT_HAVE_UUID)

 /* Flags for each dm_task type indicating the expected output.
 *  Task flags are set in `DmTask.run()`. Entries in DM_DEVICE_* enum
 *  order.
 */
static const uint32_t _DmTask_task_type_flags[] = {
    DMT_HAVE_IDENTITY,  /* CREATE */
    DMT_HAVE_IDENTITY,  /* RELOAD */
    DMT_HAVE_IDENTITY,  /* REMOVE */
    0,              /* REMOVE_ALL */
    DMT_HAVE_IDENTITY,  /* SUSPEND */
    DMT_HAVE_IDENTITY,  /* RESUME */
    (DMT_HAVE_IDENTITY | DMT_HAVE_INFO),  /* INFO */
    (DMT_HAVE_IDENTITY | DMT_HAVE_DEPS),  /*DEPS */
    DMT_HAVE_IDENTITY,  /* RENAME */
    0,  /* VERSION */
    DMT_HAVE_IDENTITY,  /* STATUS */
    (DMT_HAVE_IDENTITY | DMT_HAVE_TABLE),  /* TABLE */
    DMT_HAVE_IDENTITY,  /* WAITEVENT */
    DMT_HAVE_NAME_LIST,  /* LIST */
    DMT_HAVE_IDENTITY,  /* CLEAR */
    0,  /* MKNODES */
    DMT_HAVE_TARGET_VERSIONS,  /* LIST_VERSIONS */
    (DMT_HAVE_IDENTITY | DMT_HAVE_MESSAGE),  /* TARGET_MSG */
    DMT_HAVE_IDENTITY  /* SET_GEOMETRY */
};

/* Names for DmTask data flags for use in exception strings. */
static const char * const _DmTask_flag_strings[] = {
    "ioctl",
    "error",
    NULL,
    NULL,
    "info",
    "name",
    "UUID",
    "dependencies",
    "name list",
    "timestamp",
    "message response",
    "table",
    "status",
    "target versions"
};

typedef struct {
    PyObject_HEAD
    struct dm_task *ob_dmt;
    DmCookieObject *ob_cookie;
    uint32_t ob_flags; /* dm_task state flags */
    int ob_task_type; /* DM_DEVICE_* type at instantiation. */
} DmTaskObject;

static PyTypeObject DmTask_Type;

#define DmTaskObject_Check(v)      (Py_TYPE(v) == &DmTask_Type)

/*
 * Check whether an ioctl has been performed, and whether `flag` is present
 * in `self->ob_flags`, and raise TypeError if either condition is not met.
 */
static int
_DmTask_check_data_flags(DmTaskObject *self, uint32_t flag, char *method)
{
    int flag_index = 0;

    if (!(self->ob_flags & DMT_DID_IOCTL)) {
        PyErr_Format(PyExc_TypeError, "DmTask(%s).%s requires ioctl data.",
                     _dm_task_type_names[self->ob_task_type], method);
        return -1;
    }

    if (!(self->ob_flags & flag)) {
        while (flag >>= 1)
            flag_index++;
        PyErr_Format(PyExc_TypeError, "DmTask(%s) does not provide "
                     "%s data.", _dm_task_type_names[self->ob_task_type],
                     _DmTask_flag_strings[flag_index]);
        return -1;
    }
    return 0;
}

static int
DmTask_init(DmTaskObject *self, PyObject *args, PyObject *kwds)
{
    int type;

    if (!PyArg_ParseTuple(args, "i:__init__", &type))
        return -1;

    self->ob_cookie = NULL;
    self->ob_flags = 0;

    if (!(self->ob_dmt = dm_task_create(type))) {
        /* FIXME: use dm_task_get_errno */
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    /* Record the DM_DEVICE_* type that this DmTask was instantiated with.
     * This may be different to the actual current value of dmt->type, since
     * the library may choose to re-write the operation internally, or even
     * satisfy it using mutliple discrete operations.
     *
     * From the caller's point of view this does not matter: we just care that
     * we know the type that was requested, in order to have the correct
     * expectations for which fields will be valid in the response.
     */
    self->ob_task_type = type;
    self->ob_flags = 0;

    if (!dm_task_enable_checks(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask checks.");
        return -1;
    }

    return 0;
}

static void
DmTask_dealloc(DmTaskObject *self)
{
    if (self->ob_dmt)
        dm_task_destroy(self->ob_dmt);
    self->ob_dmt = NULL;

    Py_XDECREF(self->ob_cookie);

    PyObject_Del(self);
}

/* DmTask methods */

static PyObject *
DmTask_set_name(DmTaskObject *self, PyObject *args)
{
    char *name;

    if (!PyArg_ParseTuple(args, "s:set_name", &name))
        return NULL;

    if (!dm_task_set_name(self->ob_dmt, name)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask name.");
        return NULL;
    }
        
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
DmTask_set_uuid(DmTaskObject *self, PyObject *args)
{
    char *uuid;

    if (!PyArg_ParseTuple(args, "s:set_uuid", &uuid))
        return NULL;

    if (!dm_task_set_uuid(self->ob_dmt, uuid)) {
        PyErr_SetString(PyExc_OSError, "failed to set DmTask name.");
        return NULL;
    }
        
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
DmTask_run(DmTaskObject *self, PyObject *args)
{
    /* DMT_DID_IOCTL does not imply success. */
    self->ob_flags |= DMT_DID_IOCTL;

    if (!(dm_task_run(self->ob_dmt))) {
        self->ob_flags |= DMT_DID_ERROR;
        errno = dm_task_get_errno(self->ob_dmt);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    /* set data flags from task type */
    self->ob_flags |= _DmTask_task_type_flags[self->ob_task_type];

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *DmTask_get_driver_version(DmTaskObject *self, PyObject *args)
{
    char version[DMPY_VERSION_BUF_LEN];

    if (_DmTask_check_data_flags(self, -1, "get_driver_version"))
        return NULL;

    if (!dm_task_get_driver_version(self->ob_dmt, version, sizeof(version))) {
        PyErr_SetString(PyExc_OSError, "Failed to get device-mapper "
                        "library version.");
        return NULL;
    } 
    return Py_BuildValue("s", version);
}

static PyObject *
DmTask_get_info(DmTaskObject *self, PyObject *args)
{
    DmInfoObject *info;

    if (_DmTask_check_data_flags(self, DMT_HAVE_INFO, "get_info"))
        return NULL;

    info = newDmInfoObject();
    if (!info)
        return NULL;

    if (!(dm_task_get_info(self->ob_dmt, &info->ob_info))) {
        Py_DECREF(info);
        Py_INCREF(Py_None);
        return Py_None;
    }

    return (PyObject *)info;
}

static PyObject *DmTask_get_uuid(DmTaskObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"mangled", NULL};
    int mangled = -1; /* use name_mangling_mode */
    const char *uuid;

    if (_DmTask_check_data_flags(self, DMT_HAVE_UUID, "get_uuid"))
        return NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:get_uuid",
                                     kwlist, &mangled))
        return NULL;

    if (mangled < 0)
        uuid = dm_task_get_uuid(self->ob_dmt);
    else if (mangled)
        uuid = dm_task_get_uuid_mangled(self->ob_dmt);
    else
        uuid = dm_task_get_uuid_unmangled(self->ob_dmt);

    return Py_BuildValue("s", uuid);
}

static PyObject *
_dm_build_deps_list(struct dm_deps *deps)
{
    PyObject *deps_list = NULL, *value;
    uint64_t dev;
    unsigned i;

    if (!deps->count) {
        PyErr_SetString(PyExc_OSError, "Received empty dependency list "
                        "from device-mapper.");
        return NULL;
    }

    if (!(deps_list = PyList_New(0)))
        return NULL;

    for (i = 0; i < deps->count; i++) {
        dev = deps->device[i];
        value = Py_BuildValue("(ii)", (int) MAJOR(dev), (int) MINOR(dev));
        if (!value)
            goto fail;
        PyList_Append(deps_list, value);
    }

    return deps_list;        

fail:
    Py_DECREF(deps_list);
    return NULL;
}

static PyObject *
DmTask_get_deps(DmTaskObject *self, PyObject *args)
{
    struct dm_deps *deps = NULL;

    if (_DmTask_check_data_flags(self, DMT_HAVE_DEPS, "get_deps"))
        return NULL;

    if (!(deps = dm_task_get_deps(self->ob_dmt))) {
        PyErr_SetString(PyExc_OSError, "Failed to retrieve dependencies from "
                        "device-mapper.");
        return NULL;
    }

    return _dm_build_deps_list(deps);
}

static PyObject *
_DmTask_build_versions_dict(struct dm_versions *target)
{
    PyObject *versions_dict = NULL, *name, *value;
    struct dm_versions *last_target;
    unsigned major, minor, patch;

    if (!target->name) {
        PyErr_SetString(PyExc_OSError, "Received empty versions list "
                        "from device-mapper");
        return NULL;
    }

    if (!(versions_dict = PyDict_New()))
        return NULL;

    /* Fetch targets and print 'em */
    do {
        last_target = target;

        major = target->version[0];
        minor = target->version[1];
        patch = target->version[2];

        name = Py_BuildValue("s", target->name);
        if (!name)
            goto fail;

        value = Py_BuildValue("(iii)", major, minor, patch);
        if (!value) {
            Py_DECREF(name);
            goto fail;
        }

        if (PyDict_SetItem(versions_dict, name, value)) {
            Py_DECREF(name);
            Py_DECREF(value);
            goto fail;
        }

        target = (struct dm_versions *)((char *) target + target->next);
    } while (last_target != target);

    return versions_dict;        

fail:
    Py_DECREF(versions_dict);
    return NULL;
}

static PyObject *
DmTask_get_versions(DmTaskObject *self, PyObject *args)
{
    struct dm_versions *versions;

    if (_DmTask_check_data_flags(self, DMT_HAVE_TARGET_VERSIONS,
                                 "get_versions"))
        return NULL;

    versions = dm_task_get_versions(self->ob_dmt);
    if (!versions) {
        PyErr_SetString(PyExc_OSError, "Failed to get task versions "
                        "from device-mapper");
        return NULL;
    }
    return _DmTask_build_versions_dict(versions);
}

static PyObject *
DmTask_get_message_response(DmTaskObject *self, PyObject *args)
{
    if (_DmTask_check_data_flags(self, DMT_HAVE_MESSAGE,
                                 "get_message_response"))
        return NULL;

    return Py_BuildValue("s", dm_task_get_message_response(self->ob_dmt));
}

static PyObject *
DmTask_get_name(DmTaskObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"mangled", NULL};
    int mangled = -1; /* use name_mangling_mode */
    const char *name;

    if (_DmTask_check_data_flags(self, DMT_HAVE_NAME, "name"))
        return NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:get_name",
                                     kwlist, &mangled))
        return NULL;

    if (mangled < 0)
        name = dm_task_get_name(self->ob_dmt);
    else if (mangled)
        name = dm_task_get_name_mangled(self->ob_dmt);
    else
        name = dm_task_get_name_unmangled(self->ob_dmt);

    return Py_BuildValue("s", name);
}

static PyObject *
_DmTask_build_name_list(struct dm_names *names)
{
    PyObject *name_list = NULL;
    unsigned next = 0;

    if (!names->name) {
        PyErr_SetString(PyExc_OSError, "Received empty device list from "
                        "device-mapper");
        goto fail;
    }

    if (!(name_list = PyList_New(0)))
        return NULL;

    do {
        names = (struct dm_names *)((char *) names + next);
        PyList_Append(name_list, Py_BuildValue("(sii)", names->name,
                                               MAJOR(names->dev),
                                               MINOR(names->dev)));
        next = names->next;
    } while (next);

    return name_list;        

fail:
    Py_DECREF(name_list);
    return NULL;
}

static PyObject *
DmTask_get_names(DmTaskObject *self, PyObject *args)
{
    struct dm_names *names = NULL;

    if (_DmTask_check_data_flags(self, DMT_HAVE_NAME_LIST, "get_names"))
        return NULL;

    if (!(names = dm_task_get_names(self->ob_dmt))) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return _DmTask_build_name_list(names);
}

static PyObject *
DmTask_set_ro(DmTaskObject *self, PyObject *args)
{
    dm_task_set_ro(self->ob_dmt);
    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_set_newname(DmTaskObject *self, PyObject *args)
{
    char *newname;

    if (!PyArg_ParseTuple(args, "s:set_newname", &newname))
        goto fail;

    /* repeat the libdm validation so that a meaningful error is given. */
    if (strchr(newname, '/')) {
        PyErr_Format(PyExc_ValueError, "Name \"%s\" invalid. It contains "
                     "\"/\".", newname);
        goto fail;
    }

    if (strlen(newname) >= DM_NAME_LEN) {
        PyErr_Format(PyExc_ValueError, "Name \"%s\" too long.", newname);
        goto fail;
    }

    if (!*newname) {
        PyErr_SetString(PyExc_ValueError, "Non empty new name is required.");
        goto fail;
    }

    if (!dm_task_set_newname(self->ob_dmt, newname)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask new name.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_newuuid(DmTaskObject *self, PyObject *args)
{
    char *newuuid;

    if (!PyArg_ParseTuple(args, "s:set_newuuid", &newuuid))
        goto fail;

    if (strlen(newuuid) >= DM_UUID_LEN) {
        PyErr_Format(PyExc_ValueError, "New uuid \"%s\" too long.", newuuid);
        goto fail;
    }

    if (!dm_task_set_newuuid(self->ob_dmt, newuuid)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask new uuid.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_major(DmTaskObject *self, PyObject *args)
{
    int major;

    if (!PyArg_ParseTuple(args, "i:set_major", &major))
        goto fail;

    if (!dm_task_set_major(self->ob_dmt, major)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask major number.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_minor(DmTaskObject *self, PyObject *args)
{
    int minor;

    if (!PyArg_ParseTuple(args, "i:set_minor", &minor))
        goto fail;

    if (!dm_task_set_minor(self->ob_dmt, minor)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask minor number.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_major_minor(DmTaskObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"major", "minor", "allow_fallback", NULL};
    int major, minor, allow_fallback = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ii|i:set_major_minor", kwlist,
        &major, &minor, &allow_fallback))
        goto fail;

    if (!dm_task_set_major_minor(self->ob_dmt, major, minor, allow_fallback)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask major and "
                        "minor numbers.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_uid(DmTaskObject *self, PyObject *args)
{
    uid_t uid;

    if (!PyArg_ParseTuple(args, "i:set_uid", &uid))
        goto fail;

    if (!dm_task_set_uid(self->ob_dmt, uid)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask uid.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_gid(DmTaskObject *self, PyObject *args)
{
    gid_t gid;

    if (!PyArg_ParseTuple(args, "i:set_gid", &gid))
        goto fail;

    if (!dm_task_set_gid(self->ob_dmt, gid)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask gid.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_mode(DmTaskObject *self, PyObject *args)
{
    mode_t mode;

    if (!PyArg_ParseTuple(args, "i:set_mode", &mode))
        goto fail;

    if (!dm_task_set_mode(self->ob_dmt, mode)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask mode.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_set_cookie(DmTaskObject *self, PyObject *args)
{
    DmCookieObject *cookie = NULL;
    uint16_t flags = 0;

    if (!PyArg_ParseTuple(args, "O!:set_cookie", &DmCookie_Type, &cookie))
        return NULL;

    /* The DmTask holds a reference to the cookie object. */
    Py_INCREF(cookie);
    self->ob_cookie = cookie;

    if (!dm_task_set_cookie(self->ob_dmt, &cookie->ob_cookie, flags)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask cookie.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    Py_XDECREF(cookie);
    return NULL;
}

static PyObject *
DmTask_set_event_nr(DmTaskObject *self, PyObject *args)
{
    int event_nr;

    if (!PyArg_ParseTuple(args, "i:set_event_nr", &event_nr))
        return NULL;

    if (!dm_task_set_event_nr(self->ob_dmt, event_nr)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask event_nr.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_set_geometry(DmTaskObject *self, PyObject *args)
{
    char *cylinders, *heads, *sectors, *start;

    if (!PyArg_ParseTuple(args, "ssss:set_geometry", &cylinders, &sectors,
                          &heads, &start))

    if (!dm_task_set_geometry(self->ob_dmt, cylinders, sectors, heads, start)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask geometry,");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_set_message(DmTaskObject *self, PyObject *args)
{
    char *message;

    if (!PyArg_ParseTuple(args, "s:set_message", &message))
        return NULL;

    if (!dm_task_set_message(self->ob_dmt, message)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask message.");
        return NULL;
    }
    Py_INCREF(self);
    return (PyObject *) self;
}

static PyObject *
DmTask_set_sector(DmTaskObject *self, PyObject *args)
{
    int sector;

    if (!PyArg_ParseTuple(args, "i:set_sector", &sector))
        return NULL;

    if (!dm_task_set_sector(self->ob_dmt, sector)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask event_nr.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_no_flush(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_no_flush(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask no_flush.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_no_open_count(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_no_open_count(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask no_open_count.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_skip_lockfs(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_skip_lockfs(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask skip_lockfs.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_query_inactive_table(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_query_inactive_table(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask "
                        "query_inactive_table.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_suppress_identical_reload(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_suppress_identical_reload(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask "
                        "suppress_identical_reload.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_secure_data(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_secure_data(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask secure_data.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_retry_remove(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_retry_remove(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask retry_remove.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_deferred_remove(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_deferred_remove(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask deferred_remove.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_set_record_timestamp(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_set_record_timestamp(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask record_timestamp.");
        return NULL;
    }

    self->ob_flags |= DMT_HAVE_TIMESTAMP;

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_get_ioctl_timestamp(DmTaskObject *self, PyObject *args)
{
    struct dm_timestamp *ts;
    DmTimestampObject *new_ts;

    /* Timestamps can be enabled for any task type (if the device-mapper
     * library supports them), so test first whether an ioctl has been run,
     * and then test for the DMT_HAVE_TIMESTAMP flag separately.
     */
    if (_DmTask_check_data_flags(self, -1, "get_ioctl_timestamp"))
        return NULL;

    if (!(self->ob_flags & DMT_HAVE_TIMESTAMP)) {
        PyErr_SetString(PyExc_TypeError, "DmTask timestamps not enabled.");
        return NULL;
    }

    if (!(ts = dm_task_get_ioctl_timestamp(self->ob_dmt))) {
        PyErr_SetString(PyExc_OSError, "Failed to get ioctl timestamp from "
                        "device-mapper.");
        return NULL;
    }

    new_ts = newDmTimestampObject();
    if (!new_ts)
        return NULL;

    /* Make a copy, as ts is allocated in the ioctl buffer. */
    dm_timestamp_copy(new_ts->ob_ts, ts);

    return (PyObject *) new_ts;
}

static PyObject *
DmTask_enable_checks(DmTaskObject *self, PyObject *args)
{
    if (!dm_task_enable_checks(self->ob_dmt)) {
        PyErr_SetString(PyExc_OSError, "Failed to enable device-mapper "
                        "task checks.");
        return NULL;
    }
    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_set_add_node(DmTaskObject *self, PyObject *args)
{
    int add_node;

    if (!PyArg_ParseTuple(args, "i:set_add_node", &add_node))
        return NULL;

    if (!dm_task_set_add_node(self->ob_dmt, (dm_add_node_t) add_node)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask add_node.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_set_read_ahead(DmTaskObject *self, PyObject *args)
{
    unsigned read_ahead, read_ahead_flags;

    if (!PyArg_ParseTuple(args, "ii:set_read_ahead",
                         &read_ahead, &read_ahead_flags))
        goto fail;

    if (read_ahead > UINT32_MAX) {
        PyErr_SetString(PyExc_ValueError, "Read ahead value out of range.");
        goto fail;
    }

    if (read_ahead_flags > UINT32_MAX) {
        PyErr_SetString(PyExc_ValueError, "Read ahead flags out of range.");
        goto fail;
    }

    if (!dm_task_set_read_ahead(self->ob_dmt, (uint32_t) read_ahead,
                                (uint32_t) read_ahead_flags)) {
        PyErr_SetString(PyExc_OSError, "Failed to set DmTask read ahead.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
DmTask_add_target(DmTaskObject *self, PyObject *args)
{
    uint64_t start, size;
    const char *ttype, *params;

    if (!PyArg_ParseTuple(args, "llss:add_target", &start, &size,
                          &ttype, &params))
        return NULL;

    if (!dm_task_add_target(self->ob_dmt, start, size, ttype, params)) {
        PyErr_SetString(PyExc_OSError, "Failed to add target to DmTask.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_get_errno(DmTaskObject *self, PyObject *args)
{
    if (_DmTask_check_data_flags(self, -1, "get_errno"))
        return NULL;

    if (!(self->ob_flags && DMT_DID_ERROR))
        return 0;

    return Py_BuildValue("i", dm_task_get_errno(self->ob_dmt));
}

#define DMTASK_set_name__doc__ \
"Set the device-mapper name of this `DmTask`."

#define DMTASK_set_uuid__doc__ \
"Set the device-mapper UUID of this `DmTask`."

#define DMTASK_run__doc__ \
"Run the task associated with this `DmTask`."

#define DMTASK_get_driver_version__doc__  \
"Get the version of the device-mapper driver in use from this `DmTask`."

#define DMTASK_get_info__doc__ \
"Get the DmInfo object associated with this `DmTask`, or None if no " \
"DM_DEVICE_INFO was performed."

#define DMTASK_get_uuid__doc__                                           \
"Get the dm device's UUID based on the value of the mangling mode\nset " \
"during preceding `DmTask.run()` call.\n\nTo force either mangled or "   \
"unmangled output, set the `mangle` kwarg\nto '1' or '0' respectively."

#define DMTASK_get_deps__doc__ \
"Get the list of dependencies for the dm device."

#define DMTASK_get_versions__doc__ \
"Get target version dictionary following a `DM_DEVICE_LIST_VERSIONS`\n"    \
"command.\n\nReturns a dictionary whose keys are device-mapper target "    \
"names, and whose\nvalues are 3-tuples giving the target version numbers." \

#define DMTASK_get_message_response__doc__ \
"Retrieve the response to a prior `DM_DEVICE_TARGET_MSG` as a string."

#define DMTASK_get_name__doc__ \
"Get the device name associated with this `DmTask`."

#define DMTASK_get_names__doc__ \
"Get the list of device names returned by a prior DM_DEVICE_LIST operation " \
"on this `DmTask`.\n\nReturns a list of 3-tuples containing the device "     \
"name, major, and minor number."

#define DMTASK_set_ro__doc__ \
"Set the read-only flag in this `DmTask`."

#define DMTASK_set_newname__doc__ \
"Set the new name for a `DM_DEVICE_RENAME` task."

#define DMTASK_set_newuuid__doc__ \
"Set the new UUID for a `DM_DEVICE_RENAME` task."

#define DMTASK_set_major__doc__ \
"Set the device major number for this `DmTask`."

#define DMTASK_set_minor__doc__ \
"Set the device minor number for this `DmTask`."

#define DMTASK_set_major_minor__doc__ \
"Set the device major and minor numbers for this `DmTask`, and optionally " \
"set or clear the `allow_default_major_fallback` flag."

#define DMTASK_set_uid__doc__ \
"Set the uid for this `DmTask`."

#define DMTASK_set_gid__doc__ \
"Set the gid for this `DmTask`."

#define DMTASK_set_mode__doc__ \
"Set the mode for this `DmTask`."

#define DMTASK_set_cookie__doc__ \
"Set the udev cookie to use with this `DmTask`."

#define DMTASK_set_event_nr__doc__ \
"Set the event number for this DmTask."

#define DMTASK_set_geometry__doc__ \
"Set the device geometry (cylinders, sectors, heads, start) " \
" for this DmTask.\n\nAll arguments are given as strings."

#define DMTASK_set_message__doc__ \
"Set the message string for a DM_DEVICE_TARGET_MSG command."

#define DMTASK_set_sector__doc__ \
"Set the target sector for a DM_DEVICE_TARGET_MSG command."

#define DMTASK_no_flush__doc__ \
"Enable the no_flush flag."

#define DMTASK_no_open_count__doc__ \
"Enable the no_open_count flag."

#define DMTASK_skip_lockfs__doc__ \
"Enable the skip_lockfs flag."

#define DMTASK_query_inactive_table__doc__ \
"Enable the query_inactive_table flag."

#define DMTASK_suppress_identical_reload__doc__ \
"Enable the suppress_identical_reload flag."

#define DMTASK_secure_data__doc__ \
"Enable the secure_data flag."

#define DMTASK_retry_remove__doc__ \
"Enable the retry_remove flag."

#define DMTASK_deferred_remove__doc__ \
"Enable the deferred_remove flag."

#define DMTASK_set_record_timestamp__doc__ \
"Enable ioctl timestamps for this DmTask."

#define DMTASK_get_ioctl_timestamp__doc__ \
"Return the timestamp of the last ioctl performed by this DmTask."

#define DMTASK_enable_checks__doc__ \
"Enable checks for common mistakes such as issuing ioctls in an unsafe order."

#define DMTASK_set_add_node__doc__ \
"Set the add_node mode for this `DmTask`. If `add_node` is "       \
"`dm.ADD_NODE_ON_RESUME`,\n`/dev/mapper` nodes will be created "   \
"following dm.DM_DEVICE_RESUME.\nIf it is dm.ADD_NODE_ON_CREATE, " \
"nodes will be created following dm.DM_DEVICE_CREATE."

#define DMTASK_set_read_ahead__doc__ \
"Set the read_ahead and read_ahead_flags values for this `DmTask`."

#define DMTASK_add_target__doc__ \
"Add a target to this DmTask in preparation for a DM_DEVICE_CREATE " \
"or DM_DEVICE_RELOAD command."

#define DMTASK_get_errno__doc__ \
"The `errno` from the last device-mapper ioctl performed by `DmTask.run`."

#define DMTASK___doc__ \
""

static PyMethodDef DmTask_methods[] = {
    {"set_name", (PyCFunction)DmTask_set_name, METH_VARARGS,
        PyDoc_STR(DMTASK_set_name__doc__)},
    {"set_uuid", (PyCFunction)DmTask_set_uuid, METH_VARARGS,
        PyDoc_STR(DMTASK_set_uuid__doc__)},
    {"run", (PyCFunction)DmTask_run, METH_VARARGS,
        PyDoc_STR(DMTASK_run__doc__)},
    {"get_driver_version", (PyCFunction)DmTask_get_driver_version, METH_NOARGS,
        PyDoc_STR(DMTASK_get_driver_version__doc__)},
    {"get_info", (PyCFunction)DmTask_get_info, METH_NOARGS,
        PyDoc_STR(DMTASK_get_info__doc__)},
    {"get_uuid", (PyCFunction)DmTask_get_uuid, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR(DMTASK_get_uuid__doc__)},
    {"get_deps", (PyCFunction)DmTask_get_deps, METH_NOARGS,
        PyDoc_STR(DMTASK_get_deps__doc__)},
    {"get_versions", (PyCFunction)DmTask_get_versions, METH_NOARGS,
        PyDoc_STR(DMTASK_get_versions__doc__)},
    {"get_message_response", (PyCFunction)DmTask_get_message_response,
        METH_NOARGS, PyDoc_STR(DMTASK_get_message_response__doc__)},
    {"get_name", (PyCFunction)DmTask_get_name, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR(DMTASK_get_name__doc__)},
    {"get_names", (PyCFunction)DmTask_get_names, METH_NOARGS,
        PyDoc_STR(DMTASK_get_names__doc__)},
    {"set_ro", (PyCFunction)DmTask_set_ro, METH_NOARGS,
        PyDoc_STR(DMTASK_set_ro__doc__)},
    {"set_newname", (PyCFunction)DmTask_set_newname, METH_VARARGS,
        PyDoc_STR(DMTASK_set_newname__doc__)},
    {"set_newuuid", (PyCFunction)DmTask_set_newuuid, METH_VARARGS,
        PyDoc_STR(DMTASK_set_newuuid__doc__)},
    {"set_major", (PyCFunction)DmTask_set_major, METH_VARARGS,
        PyDoc_STR(DMTASK_set_major__doc__)},
    {"set_minor", (PyCFunction)DmTask_set_minor, METH_VARARGS,
        PyDoc_STR(DMTASK_set_minor__doc__)},
    {"set_major_minor", (PyCFunction)DmTask_set_major_minor,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(DMTASK_set_major_minor__doc__)},
    {"set_uid", (PyCFunction)DmTask_set_uid, METH_VARARGS,
        PyDoc_STR(DMTASK_set_uid__doc__)},
    {"set_gid", (PyCFunction)DmTask_set_gid, METH_VARARGS,
        PyDoc_STR(DMTASK_set_gid__doc__)},
    {"set_mode", (PyCFunction)DmTask_set_mode, METH_VARARGS,
        PyDoc_STR(DMTASK_set_mode__doc__)},
    {"set_cookie", (PyCFunction)DmTask_set_cookie, METH_VARARGS,
        PyDoc_STR(DMTASK_set_cookie__doc__)},
    {"set_event_nr", (PyCFunction)DmTask_set_event_nr, METH_VARARGS,
        PyDoc_STR(DMTASK_set_event_nr__doc__)},
    {"set_geometry", (PyCFunction)DmTask_set_geometry, METH_VARARGS,
        PyDoc_STR(DMTASK_set_geometry__doc__)},
    {"set_message", (PyCFunction)DmTask_set_message, METH_VARARGS,
        PyDoc_STR(DMTASK_set_message__doc__)},
    {"set_sector", (PyCFunction)DmTask_set_sector, METH_VARARGS,
        PyDoc_STR(DMTASK_set_sector__doc__)},
    {"no_flush", (PyCFunction)DmTask_no_flush, METH_NOARGS,
        PyDoc_STR(DMTASK_no_flush__doc__)},
    {"no_open_count", (PyCFunction)DmTask_no_open_count, METH_NOARGS,
        PyDoc_STR(DMTASK_no_open_count__doc__)},
    {"skip_lockfs", (PyCFunction)DmTask_skip_lockfs, METH_NOARGS,
        PyDoc_STR(DMTASK_skip_lockfs__doc__)},
    {"query_inactive_table", (PyCFunction)DmTask_query_inactive_table,
        METH_NOARGS, PyDoc_STR(DMTASK_query_inactive_table__doc__)},
    {"suppress_identical_reload", (PyCFunction)DmTask_suppress_identical_reload,
        METH_NOARGS, PyDoc_STR(DMTASK_suppress_identical_reload__doc__)},
    {"secure_data", (PyCFunction)DmTask_secure_data, METH_NOARGS,
        PyDoc_STR(DMTASK_secure_data__doc__)},
    {"retry_remove", (PyCFunction)DmTask_retry_remove, METH_NOARGS,
        PyDoc_STR(DMTASK_retry_remove__doc__)},
    {"deferred_remove", (PyCFunction)DmTask_deferred_remove, METH_NOARGS,
        PyDoc_STR(DMTASK_deferred_remove__doc__)},
    {"set_record_timestamp", (PyCFunction)DmTask_set_record_timestamp, METH_NOARGS,
        PyDoc_STR(DMTASK_set_record_timestamp__doc__)},
    {"get_ioctl_timestamp", (PyCFunction)DmTask_get_ioctl_timestamp, METH_NOARGS,
        PyDoc_STR(DMTASK_get_ioctl_timestamp__doc__)},
    {"enable_checks", (PyCFunction)DmTask_enable_checks, METH_NOARGS,
        PyDoc_STR(DMTASK_enable_checks__doc__)},
    {"set_add_node", (PyCFunction)DmTask_set_add_node, METH_VARARGS,
        PyDoc_STR(DMTASK_set_add_node__doc__)},
    {"set_read_ahead", (PyCFunction)DmTask_set_read_ahead, METH_VARARGS,
        PyDoc_STR(DMTASK_set_read_ahead__doc__)},
    {"add_target", (PyCFunction)DmTask_add_target, METH_VARARGS,
        PyDoc_STR(DMTASK_add_target__doc__)},
    {"get_errno", (PyCFunction)DmTask_get_errno, METH_VARARGS,
        PyDoc_STR(DMTASK_get_errno__doc__)},
    {NULL, NULL}           /* sentinel */
};

static PyTypeObject DmTask_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmTask",             /*tp_name*/
    sizeof(DmTaskObject),          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)DmTask_dealloc,    /*tp_dealloc*/
    0,                             /*tp_print*/
    0,                             /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    DmTask_methods,                /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmTask_init,      /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};


/*
 * DmStats objects.
 */

typedef struct {
    PyObject_HEAD
    struct dm_stats *ob_dms;
    uint64_t ob_sequence; /* sequence number protecting ob_dms */
    PyObject **ob_regions; /* region cache */
    Py_ssize_t ob_regions_len; /* length of the region cache in regions. */
} DmStatsObject;

static PyTypeObject DmStats_Type;

#define DmStatsObject_Check(v)      (Py_TYPE(v) == &DmStats_Type)

typedef struct {
    PyObject_HEAD
    PyObject *ob_stats;
    uint64_t ob_sequence;
    uint64_t ob_region_id;
    PyObject *ob_weakreflist;
    PyObject **ob_areas;
    Py_ssize_t ob_areas_len;
} DmStatsRegionObject;

static PyTypeObject DmStatsRegion_Type;

#define DmStatsRegionObject_Check(v)      (Py_TYPE(v) == &DmStatsRegion_Type)

#define DMSTATS_FROM_REGION(r) ((DmStatsObject *)((r)->ob_stats))
#define DMS_FROM_REGION(r) (DMSTATS_FROM_REGION((r))->ob_dms)

typedef struct {
    PyObject_HEAD
    PyObject *ob_stats;
    PyObject *ob_weakreflist;
    uint64_t ob_area_id;
} DmStatsAreaObject;

static PyTypeObject DmStatsArea_Type;

#define DmStatsAreaObject_Check(v)      (Py_TYPE(v) == &DmStatsArea_Type)

static void
DmStats_dealloc(DmStatsObject *self)
{
    if (self->ob_dms)
        dm_stats_destroy(self->ob_dms);
    self->ob_dms = NULL;
    if (self->ob_regions) {
        PyMem_Free(self->ob_regions);
        self->ob_regions = NULL;
        self->ob_regions_len = 0;
    }
    self->ob_sequence = UINT64_MAX;
    Py_TYPE(self)->tp_free(self);
}

static PyObject *
DmStats_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    DmStatsObject *obj;

    if (type == &DmStats_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (!(obj = (DmStatsObject *) type->tp_alloc(&DmStats_Type, 0)))
        return NULL;

    obj->ob_dms = NULL;
    obj->ob_sequence = 0;
    obj->ob_regions = NULL;
    obj->ob_regions_len = 0;

    return (PyObject *) obj;
}

#define DMSTATS__init__KWARG_ERR "Please specify one of name=, uuid=, or " \
"major= and minor= keyword arguments."
static int
DmStats_init(DmStatsObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"program_id",
                             "name", "uuid", "major", "minor", NULL};
    const char *program_id = NULL, *name = NULL, *uuid = NULL;
    int major = 0, minor = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "z|ssii:__init__", kwlist,
                                     &program_id, &name, &uuid, &major, &minor))
        return -1;

    if (name) {
        if (uuid || major || minor) {
            PyErr_SetString(PyExc_TypeError, DMSTATS__init__KWARG_ERR);
            return -1;
        }
    }

    if (uuid) {
        if (major || minor) {
            PyErr_SetString(PyExc_TypeError, DMSTATS__init__KWARG_ERR);
            return -1;
        }
    }

    self->ob_dms = dm_stats_create(program_id);

    if (!self->ob_dms) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocated "
                        "DmStats handle.");
        return -1;
    }

    if (name) {
        if (!dm_stats_bind_name(self->ob_dms, name)) {
            PyErr_SetString(PyExc_OSError, "Failed to bind name to "
                            "DmStatst handle.");
            goto fail;
        }
    } else if (uuid) {
        if (!dm_stats_bind_uuid(self->ob_dms, uuid)) {
            PyErr_SetString(PyExc_OSError, "Failed to bind uuid to "
                            "DmStatst handle.");
            goto fail;
        }
    } else if (major) {
        if (!minor) {
            PyErr_SetString(PyExc_ValueError,"Missing minor= keyword "
                                             "argument.");
            goto fail;
        }
        if (!dm_stats_bind_devno(self->ob_dms, major, minor)) {
            PyErr_SetString(PyExc_OSError, "Failed to bind devno to "
                            "DmStatst handle.");
            goto fail;
        }
    } else if (minor) {
            PyErr_SetString(PyExc_ValueError,"Missing major= keyword "
                                             "argument.");
            goto fail;
    }

    return 0;
fail:
    dm_stats_destroy(self->ob_dms);
    return -1;
}

static int
DmStats_traverse(DmStatsObject *self, visitproc visit, void *arg)
{
    int i;

    if (!self->ob_regions)
        return 0;

    for (i = 0; i < self->ob_regions_len; i++)
        Py_VISIT(self->ob_regions[i]);
    return 0;
}

static int
DmStats_clear(DmStatsObject *self)
{
    int i;

    if (!self->ob_regions)
        return 0;

    for (i = 0; i < self->ob_regions_len; i++)
        Py_CLEAR(self->ob_regions[i]);
    return 0;
}

/*
 * DmStats Sequence Methods.
 */

Py_ssize_t
DmStats_len(PyObject *o)
{
    DmStatsObject *self = (DmStatsObject *) o;

    if (!DmStatsObject_Check(o))
        return -1;

    if (!self->ob_dms)
        return 0;

    return (Py_ssize_t) self->ob_regions_len;
}

static DmStatsRegionObject *
newDmStatsRegionObject(PyObject *stats, uint64_t region_id, uint64_t sequence);

static void
_DmStatsRegion_clear_area_cache(DmStatsRegionObject *self)
{
    int64_t i;

    if (!self->ob_areas)
        return;

    /* If an area is in the cache, we are holding a reference
     * to the Weakref object that represents it.
     */
    if (self->ob_areas_len) {
        for (i = 0; i < self->ob_areas_len; i++)
            Py_XDECREF(self->ob_areas[i]);
    }
    PyMem_Free(self->ob_areas);
    self->ob_areas = NULL;
    self->ob_areas_len = 0;
}

static void
_DmStatsRegion_set_area_cache(DmStatsRegionObject *self)
{
    struct dm_stats *dms = DMS_FROM_REGION(self);
    uint64_t nr_slots;

    nr_slots = dm_stats_get_region_nr_areas(dms, self->ob_region_id);
    if (nr_slots) {
        self->ob_areas = PyMem_Malloc(sizeof(PyObject *) * nr_slots);
        self->ob_areas_len = nr_slots;
        memset(self->ob_areas, 0, nr_slots * sizeof(PyObject *));
    }
}

static PyObject *
DmStats_get_item(PyObject *o, Py_ssize_t i)
{
    DmStatsObject *self = (DmStatsObject *) o;
    PyObject *region;

    if (!DmStatsObject_Check(o))
        return NULL;

    if ((i < 0) || (i >= self->ob_regions_len)) {
        PyErr_SetString(PyExc_IndexError, "DmStats region_id out of range");
        return NULL;
    }

    if (!dm_stats_region_present(self->ob_dms, i)) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (!self->ob_regions[i]) {
cache_new:
        /* cache miss */
        region = (PyObject *) newDmStatsRegionObject(o, i, self->ob_sequence);
        self->ob_regions[i] = PyWeakref_NewRef(region, NULL);
        _DmStatsRegion_set_area_cache((DmStatsRegionObject *) region);
    } else {
        region = PyWeakref_GetObject(self->ob_regions[i]);
        if (region == Py_None) {
            /* cache hit but referent expired */
            Py_DECREF(self->ob_regions[i]);
            self->ob_regions[i] = NULL;
            goto cache_new;
        }
        /* cache hit */
        Py_INCREF(region);
    }

    return region;
}

static PySequenceMethods DmStats_sequence_methods = {
    DmStats_len,
    0,
    0,
    DmStats_get_item
};

PyObject *DmStats_bind_devno(DmStatsObject *self, PyObject *args)
{
    int major, minor;

    if (!PyArg_ParseTuple(args, "ii:bind_devno", &major, &minor))
        return NULL;

    if (!dm_stats_bind_devno(self->ob_dms, major, minor)) {
        PyErr_SetString(PyExc_OSError, "Failed to bind DmStats to devno.");
        return NULL;
    }

    self->ob_sequence++;
    Py_INCREF(Py_True);
    return Py_True;
}

PyObject *DmStats_bind_name(DmStatsObject *self, PyObject *args)
{
    char *name;

    if (!PyArg_ParseTuple(args, "s:bind_name", &name))
        return NULL;

    if (!name || !strlen(name)) {
        PyErr_SetString(PyExc_ValueError, "DmStats name cannot be empty "
                        "or None.");
        return NULL;
    }

    if (!dm_stats_bind_name(self->ob_dms, name)) {
        PyErr_SetString(PyExc_OSError, "Failed to bind DmStats to name.");
        return NULL;
    }

    self->ob_sequence++;
    Py_INCREF(Py_True);
    return Py_True;
}

PyObject *DmStats_bind_uuid(DmStatsObject *self, PyObject *args)
{
    char *uuid;

    if (!PyArg_ParseTuple(args, "s:bind_uuid", &uuid))
        return NULL;

    if (!uuid || !strlen(uuid)) {
        PyErr_SetString(PyExc_ValueError, "DmStats uuid cannot be empty "
                        "or None.");
        return NULL;
    }

    if (!dm_stats_bind_uuid(self->ob_dms, uuid)) {
        PyErr_SetString(PyExc_OSError, "Failed to bind DmStats to uuid.");
        return NULL;
    }

    self->ob_sequence++;
    Py_INCREF(Py_True);
    return Py_True;
}

/*
 * DmStats property methods
 * FIXME: expose these via the object's __dict__
 */

PyObject *DmStats_nr_regions(DmStatsObject *self, PyObject *args)
{
    return Py_BuildValue("i", dm_stats_get_nr_regions(self->ob_dms));
}

PyObject *DmStats_nr_groups(DmStatsObject *self, PyObject *args)
{
    return Py_BuildValue("i", dm_stats_get_nr_groups(self->ob_dms));
}

PyObject *DmStats_region_present(DmStatsObject *self, PyObject *args)
{
    int region_id;

    if (!PyArg_ParseTuple(args, "i:region_present", &region_id))
        return NULL;

    return Py_BuildValue("i", dm_stats_region_present(self->ob_dms,
                                                      region_id));
}

PyObject *DmStats_region_nr_areas(DmStatsObject *self, PyObject *args)
{
    int region_id, val;

    if (!PyArg_ParseTuple(args, "i:region_nr_areas", &region_id))
        return NULL;

    /* FIXME: dm_stats_get_region_nr_areas segfaults on an un-listed handle */
    if (dm_stats_get_nr_areas(self->ob_dms))
        val = dm_stats_get_region_nr_areas(self->ob_dms, region_id);
    else
        val = 0;

    return Py_BuildValue("i", val);
}

PyObject *DmStats_nr_areas(DmStatsObject *self, PyObject *args)
{
    return Py_BuildValue("i", dm_stats_get_nr_areas(self->ob_dms));
}

PyObject *DmStats_group_present(DmStatsObject *self, PyObject *args)
{
    int group_id, val;

    if (!PyArg_ParseTuple(args, "i:group_present", &group_id))
        return NULL;

    val = dm_stats_group_present(self->ob_dms, group_id);
    return Py_BuildValue("i", val);
}

PyObject *DmStats_set_sampling_interval(DmStatsObject *self, PyObject *args)
{
    uint64_t interval_ns;
    double interval;

    if (!PyArg_ParseTuple(args, "d:set_sampling_interval", &interval))
        return NULL;

    interval_ns = (uint64_t)(interval * (double) NSEC_PER_SEC);
    dm_stats_set_sampling_interval_ns(self->ob_dms, interval_ns);

    Py_INCREF(Py_True);
    return Py_True;
}

PyObject *DmStats_get_sampling_interval(DmStatsObject *self, PyObject *args)
{
    uint64_t interval_ns;
    double interval;

    interval_ns = dm_stats_get_sampling_interval_ns(self->ob_dms);
    interval = ((double) interval_ns / (double) NSEC_PER_SEC);

    return Py_BuildValue("d", interval);
}

PyObject *DmStats_set_program_id(DmStatsObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"program_id", "allow_empty", NULL};
    int allow_empty = 0;
    char *program_id;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "z|i:set_program_id", kwlist,
                                     &program_id, &allow_empty))
        return NULL;

    if (!allow_empty && (!program_id || !strlen(program_id))) {
        PyErr_SetString(PyExc_ValueError, "Empty program_id not permitted "
                        "without allow_empty=True.");
        return NULL;
    }
    if (!dm_stats_set_program_id(self->ob_dms, allow_empty, program_id)) {
        PyErr_SetString(PyExc_OSError, "Failed to set program_id.");
        return NULL;
    }
    Py_INCREF(Py_True);
    return Py_True;
}

void
_DmStats_clear_region_cache(DmStatsObject *self)
{
    PyObject *region;
    int64_t i;

    if (!self->ob_regions)
        return;

    /* If a region is in the cache, we are holding a reference
     * to the Weakref object that represents it: if the reference is
     * still alive, retrive the object and clear its area cache.
     */
    if (self->ob_regions_len) {
        for (i = 0; i < self->ob_regions_len; i++) {
            if (self->ob_regions[i]) {
                region = PyWeakref_GetObject(self->ob_regions[i]);
                if (region == Py_None)
                    continue;
                Py_INCREF(region);
                _DmStatsRegion_clear_area_cache((DmStatsRegionObject *) region);
                Py_DECREF(region);
            }
            Py_XDECREF(self->ob_regions[i]);
        }
    }
    PyMem_Free(self->ob_regions);
    self->ob_regions = NULL;
    self->ob_regions_len = 0;
}

void
_DmStats_set_region_cache(DmStatsObject *self)
{
    struct dm_stats *dms;
    uint64_t nr_slots, region_id, max_region = 0;

    dms = self->ob_dms;

    if (dm_stats_get_nr_areas(dms)) {
        dm_stats_foreach_region(dms) {
            region_id = dm_stats_get_current_region(dms);
            max_region = (region_id > max_region) ? region_id : max_region;
        }
        nr_slots = max_region + 1;
        self->ob_regions = PyMem_Malloc(sizeof(PyObject *) * nr_slots);
        self->ob_regions_len = nr_slots;
        memset(self->ob_regions, 0, nr_slots * sizeof(PyObject *));
    }
}

PyObject *
DmStats_list(DmStatsObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"program_id", NULL};
    char *program_id = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|z:list",
                                     kwlist, &program_id))
        return NULL;

    _DmStats_clear_region_cache(self);

    if (!dm_stats_list(self->ob_dms, program_id)) {
        PyErr_SetString(PyExc_OSError, "Failed to get region list from "
                        "device-mapper.");
        return NULL;
    }
    self->ob_sequence++;
    _DmStats_set_region_cache(self);

    Py_INCREF(self);
    return (PyObject *) self;
}

PyObject *
DmStats_populate(DmStatsObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"program_id", "region_id", NULL};
    uint64_t region_id = DM_STATS_REGIONS_ALL;
    char *program_id = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|zl:list",
                                     kwlist, &program_id, &region_id))
        return NULL;

    _DmStats_clear_region_cache(self);
    if (!dm_stats_populate(self->ob_dms, program_id, region_id)) {
        PyErr_SetString(PyExc_OSError, "Failed to get region data from "
                        "device-mapper.");
        return NULL;
    }
    self->ob_sequence++;
    _DmStats_set_region_cache(self);

    Py_INCREF(self);
    return (PyObject *) self;
}

#define DMSTATS_bind_devno__doc__ \
"Bind a DmStats object to the specified device major and minor values.\n" \
"Any previous binding is cleared and any preexisting counter data\n"      \
"contained in the object is released."

#define DMSTATS_bind_name__doc__ \
"Bind a DmStats object to the specified device name. Any previous\n" \
"binding is cleared and any preexisting counter data contained in\n" \
"the object is released."

#define DMSTATS_bind_uuid__doc__ \
"Bind a DmStats object to the specified device UUID. Any previous\n" \
"binding is cleared and any preexisting counter data contained in\n" \
"the object is released."

#define DMSTATS_nr_regions__doc__ \
"Return the number of regions present in this DmStats object."

#define DMSTATS_nr_groups__doc__ \
"Return the number of groups present in this DmStats object."

#define DMSTATS_region_present__doc__ \
"Test whether region_id is present in this DmStats object."

#define DMSTATS_region_nr_areas__doc__ \
"Return the number of areas present in region_id."

#define DMSTATS_nr_areas__doc__ \
"Return the number of areas present in this DmStats object."

#define DMSTATS_group_present__doc__ \
"Test whether group_id is present in this DmStats object."

#define DMSTATS_set_sampling_interval__doc__ \
"Set the sampling interval for counter data to the specified value in\n" \
"seconds. Floating point values are permitted.\n\n"                      \
"The interval is used to calculate time-based metrics from the basic\n"  \
"counter data: an interval must be set before calling any of the\n"      \
"metric methods.\n\n"                                                    \
"For best accuracy the duration should be measured and updated at the\n" \
"end of each interval.\n\n"                                              \
"All values are stored internally with nanosecond precision and are\n"   \
"converted to or from seconds as needed."

#define DMSTATS_get_sampling_interval__doc__ \
"Return the currently configured sampling interval in seconds, as a\n"   \
"Floating point value."

#define DMSTATS_set_program_id__doc__ \
"Override program_id. This may be used to change the default\n"          \
"program_id value for an existing handle. If the allow_empty argument\n" \
"is non-zero a program_id of None or the empty string is permitted.\n\n" \
"Use with caution! Most users of the library should set a valid,\n"      \
"non-NULL program_id for every statistics region created. Failing to\n"  \
"do so may result in confusing state when multiple programs are\n"       \
"creating and managing statistics regions.\n\n"                          \
"All users of the library are encouraged to choose an unambiguous,\n"    \
"unique program_id: this could be based on PID (for programs that\n"     \
"create, report, and delete regions in a single process), session id,\n" \
"executable name, or some other distinguishing string.\n\n"              \
"Use of the empty string as a program_id does not simplify use of the\n" \
"library or the command line tools and use of this value is strongly\n"  \
"discouraged."

#define DMSTATS_list__doc__ \
"Send a @stats_list message, and parse the result into this DmStats\n"   \
"object."

#define DMSTATS_populate__doc__ \
"Populate this DmStats object with data from device-mapper."

#define DMSTATS___doc__ \
""

static PyMethodDef DmStats_methods[] = {
    {"bind_devno", (PyCFunction)DmStats_bind_devno, METH_VARARGS,
        PyDoc_STR(DMSTATS_bind_devno__doc__)},
    {"bind_name", (PyCFunction)DmStats_bind_name, METH_VARARGS,
        PyDoc_STR(DMSTATS_bind_name__doc__)},
    {"bind_uuid", (PyCFunction)DmStats_bind_uuid, METH_VARARGS,
        PyDoc_STR(DMSTATS_bind_uuid__doc__)},
    {"nr_regions", (PyCFunction)DmStats_nr_regions, METH_NOARGS,
        PyDoc_STR(DMSTATS_nr_regions__doc__)},
    {"nr_groups", (PyCFunction)DmStats_nr_groups, METH_NOARGS,
        PyDoc_STR(DMSTATS_nr_groups__doc__)},
    {"nr_areas", (PyCFunction)DmStats_nr_areas, METH_NOARGS,
        PyDoc_STR(DMSTATS_nr_areas__doc__)},
    {"region_present", (PyCFunction)DmStats_region_present, METH_VARARGS,
        PyDoc_STR(DMSTATS_region_present__doc__)},
    {"region_nr_areas", (PyCFunction)DmStats_region_nr_areas, METH_VARARGS,
        PyDoc_STR(DMSTATS_region_nr_areas__doc__)},
    {"group_present", (PyCFunction)DmStats_group_present, METH_VARARGS,
        PyDoc_STR(DMSTATS_group_present__doc__)},
    {"set_sampling_interval", (PyCFunction)DmStats_set_sampling_interval,
        METH_VARARGS, PyDoc_STR(DMSTATS_set_sampling_interval__doc__)},
    {"get_sampling_interval", (PyCFunction)DmStats_get_sampling_interval,
        METH_VARARGS, PyDoc_STR(DMSTATS_get_sampling_interval__doc__)},
    {"set_program_id", (PyCFunction)DmStats_set_program_id,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(DMSTATS_set_program_id__doc__)},
    {"list", (PyCFunction)DmStats_list,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(DMSTATS_list__doc__)},
    {"populate", (PyCFunction)DmStats_populate,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(DMSTATS_populate__doc__)},
    {NULL, NULL}
};

#define DMSTATS__doc__ \
"Base class representing a device-mapper statistics handle.\n\n"            \
"Operations on dm_stats objects include managing statistics regions\n"      \
"and obtaining and manipulating current counter values from the\n"          \
"kernel. Methods are provided to return baisc count values and to\n"        \
"derive time-based metrics when a suitable interval estimate is\n"          \
"provided.\n\n"                                                             \
                                                                            \
"Internally the dm_stats handle contains a pointer to a table of one\n"     \
"or more dm_stats_region objects representing the regions registered\n"     \
"with the dm_stats_create_region() method. These in turn point to a\n"      \
"table of one or more dm_stats_counters objects containing the\n"           \
"counter sets for each defined area within the region:\n\n"                 \
                                                                            \
"dm_stats->dm_stats_region[nr_regions]->dm_stats_counters[nr_areas]\n\n"    \
                                                                            \
"This structure is private to the library and may change in future\n"       \
"versions: all users should make use of the public interface and treat\n"   \
"the dm_stats type as an opaque handle.\n\n"                                \
                                                                            \
"Regions and counter sets are stored in order of increasing region_id.\n"   \
"Depending on region specifications and the sequence of create and\n"       \
"delete operations this may not correspond to increasing sector\n"          \
"number: users of the library should not assume that this is the case\n"    \
"unless region creation is deliberately managed to ensure this (by\n"       \
"always creating regions in strict order of ascending sector address).\n\n" \
                                                                            \
"Regions may also overlap so the same sector range may be included in\n"    \
"more than one region or area: applications should be prepared to deal\n"   \
"with this or manage regions such that it does not occur."

static PyTypeObject DmStats_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmStats",             /*tp_name*/
    sizeof(DmStatsObject),      /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)DmStats_dealloc, /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    &DmStats_sequence_methods,  /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    DMSTATS__doc__,             /*tp_doc*/
    (traverseproc)DmStats_traverse, /*tp_traverse*/
    (inquiry)DmStats_clear,     /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    DmStats_methods,            /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmStats_init,     /*tp_init*/
    0,                          /*tp_alloc*/
    DmStats_new,                /*tp_new*/
    PyObject_GC_Del,            /*tp_free*/
    0,                          /*tp_is_gc*/
};

static void
DmStatsRegion_dealloc(DmStatsRegionObject *self)
{
    /* Notify our parent that we are departing. */
    if (self->ob_weakreflist)
        PyObject_ClearWeakRefs((PyObject *) self);

    /* release our reference on the parent DmStats. */
    Py_DECREF(self->ob_stats);
    return;
}

static DmStatsRegionObject *
newDmStatsRegionObject(PyObject *stats, uint64_t region_id, uint64_t sequence)
{
    DmStatsRegionObject *region;
    region = PyObject_GC_New(DmStatsRegionObject, &DmStatsRegion_Type);
    region->ob_region_id = region_id;
    region->ob_sequence = sequence;
    region->ob_weakreflist = NULL;
    region->ob_stats = stats;

    /* We keep a reference on the parent DmStats to prevent it (and its handle)
     * from being deallocated.
     */
    Py_INCREF(stats);
    return region;
}

static int
DmStatsRegion_init(DmStatsRegionObject *self, PyObject *args, PyObject *kwds)
{
    uint64_t region_id;
    if (!PyArg_ParseTuple(args, "i:__init__", &region_id))
        return -1;
    self->ob_region_id = region_id;
    return 0;
}

static int
DmStatsRegion_traverse(DmStatsRegionObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->ob_stats);
    return 0;
}

static int
DmStatsRegion_clear(DmStatsRegionObject *self)
{
    Py_CLEAR(self->ob_stats);
    return 0;
}

Py_ssize_t
DmStatsRegion_len(PyObject *o)
{
    DmStatsRegionObject *self = (DmStatsRegionObject *) o;
    struct dm_stats *dms;

    if (!DmStatsRegionObject_Check(o))
        return -1;

    dms = DMS_FROM_REGION(self);
    if (!dms)
        return 0;

    return (Py_ssize_t) dm_stats_get_region_nr_areas(dms, self->ob_region_id);
}

static DmStatsAreaObject *
newDmStatsAreaObject(PyObject *stats, uint64_t area_id);

PyObject *DmStatsRegion_get_item(PyObject *o, Py_ssize_t j)
{
    DmStatsRegionObject *self = (DmStatsRegionObject *) o;
    DmStatsObject *stats = (DmStatsObject *) self->ob_stats;
    PyObject *area;

    if (!DmStatsRegionObject_Check(o))
        return NULL;

    if ((j < 0) || (j >= self->ob_areas_len)) {
        PyErr_SetString(PyExc_IndexError, "DmStats area_id out of range");
        return NULL;
    }

    /* This return is currently unreachable since a not-present region_id
     * returns the None type for a lookup in the containing DmStats. If
     * that is later changed to return a singleton "null region", then
     * the following presence check ensures correct behaviour.
     */
    if (!dm_stats_region_present(stats->ob_dms, self->ob_region_id)) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (!self->ob_areas[j]) {
cache_new:
        /* cache miss */
        area = (PyObject *) newDmStatsAreaObject((PyObject *) stats, j);
        self->ob_areas[j] = PyWeakref_NewRef(area, NULL);
    } else {
        area = PyWeakref_GetObject(self->ob_areas[j]);
        if (area == Py_None) {
            /* cache hit but referent expired */
            Py_DECREF(self->ob_areas[j]);
            self->ob_areas[j] = NULL;
            goto cache_new;
        }
        /* cache hit */
        Py_INCREF(area);
    }

    return area;
}

static PySequenceMethods DmStatsRegion_sequence_methods = {
    DmStatsRegion_len,
    0,
    0,
    DmStatsRegion_get_item
};

/* Check the sequence number for this DmStatsRegion against its parent.
 * The ob_sequence value stored in DmStats is initialised to zero, and
 * incremented on each operation that invalidates the handle's tables
 * (bind, list, populate).
 *
 * If the sequence number we are holding, and the one stored in the
 * parent do not match, the handle has been invalidated since this
 * object was created and all operations should raise LookupError.
 */
static int
_DmStatsRegion_sequence_check(PyObject *o)
{
    DmStatsRegionObject *self = (DmStatsRegionObject *) o;
    DmStatsObject *stats;

    if (!DmStatsRegionObject_Check(o))
        return -1;

    stats = DMSTATS_FROM_REGION(self);

    if (self->ob_sequence != stats->ob_sequence) {
        PyErr_SetString(PyExc_LookupError, "Attempt to access regions in"
                        " changed DmStats object.");
        return -1;
    }
    return 0;
}

#define DmStatsRegion_SeqCheck(o)                          \
do {                                                       \
    if (_DmStatsRegion_sequence_check((PyObject *)(o)))    \
        return NULL;                                       \
} while(0);

#define DMSTATSREG___doc__ \
""

#define DMSTATSREG__doc__ \
"Base class representing a region of a device-mapper statistics handle,\n"  \
"including any areas and counters contained within it.\n\n"                 \

static PyMethodDef DmStatsRegion_methods[] = {
    {NULL, NULL}
};

static PyObject *
DmStatsRegion_nr_areas_getter(PyObject *self, void *arg)
{
    DmStatsRegionObject *reg = (DmStatsRegionObject *) self;
    DmStatsObject *stats = DMSTATS_FROM_REGION(reg);
    uint64_t nr_areas;

    DmStatsRegion_SeqCheck(self);

    nr_areas = dm_stats_get_region_nr_areas(stats->ob_dms, reg->ob_region_id);
    return Py_BuildValue("i", nr_areas);
}

static PyObject *
DmStatsRegion_present_getter(PyObject *self, void *arg)
{
    DmStatsRegionObject *reg = (DmStatsRegionObject *) self;
    struct dm_stats *dms = DMS_FROM_REGION(reg);
    PyObject *ret;

    DmStatsRegion_SeqCheck(self);

    ret = (dm_stats_region_present(dms, reg->ob_region_id))
        ? Py_True : Py_False;
    Py_INCREF(ret);
    return ret;
}

static PyObject *
DmStatsRegion_precise_timestamps_getter(PyObject *self, void *arg)
{
    DmStatsRegionObject *reg = (DmStatsRegionObject *) self;
    struct dm_stats *dms = DMS_FROM_REGION(reg);
    PyObject *ret;

    DmStatsRegion_SeqCheck(self);

    ret = (dm_stats_get_region_precise_timestamps(dms, reg->ob_region_id))
        ? Py_True : Py_False;
    Py_INCREF(ret);
    return ret;
}

#define DMSTATSREG_nr_areas_gets__doc__                \
"Return the number of areas contained in this region."

#define DMSTATSREG_present_gets__doc__                      \
"Boolean indicating whether this region is present or not."

#define DMSTATSREG_precise_timestamps_gets__doc__                            \
"Returns True if the specified region has the precise_timestamps feature\n"  \
"enabled (i.e. produces nanosecond-precision counter values) or False for\n" \
"a region using the default milisecond precision."

static PyGetSetDef DmStatsRegion_getsets[] = {
    {"present", DmStatsRegion_present_getter, NULL,
      PyDoc_STR(DMSTATSREG_present_gets__doc__), NULL},
    {"nr_areas", DmStatsRegion_nr_areas_getter, NULL,
      PyDoc_STR(DMSTATSREG_nr_areas_gets__doc__), NULL},
    {"precise_timestamps", DmStatsRegion_precise_timestamps_getter, NULL,
      PyDoc_STR(DMSTATSREG_precise_timestamps_gets__doc__), NULL},
    {NULL, NULL}
};

static PyTypeObject DmStatsRegion_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmStatsRegion",             /*tp_name*/
    sizeof(DmStatsRegionObject),      /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)DmStatsRegion_dealloc,
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    &DmStatsRegion_sequence_methods, /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    DMSTATSREG__doc__,          /*tp_doc*/
    (traverseproc) DmStatsRegion_traverse, /*tp_traverse*/
    (inquiry) DmStatsRegion_clear, /*tp_clear*/
    0,                          /*tp_richcompare*/
    offsetof(DmStatsRegionObject, ob_weakreflist), /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    DmStatsRegion_methods,      /*tp_methods*/
    0,                          /*tp_members*/
    DmStatsRegion_getsets,      /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmStatsRegion_init, /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};


static void
DmStatsArea_dealloc(DmStatsAreaObject *self)
{
    /* Notify our parent that we are departing. */
    if (self->ob_weakreflist)
        PyObject_ClearWeakRefs((PyObject *) self);

    /* release our reference on the parent DmStats. */
    Py_DECREF(self->ob_stats);
    return;
}

static DmStatsAreaObject *
newDmStatsAreaObject(PyObject *stats, uint64_t area_id)
{
    DmStatsAreaObject *area;
    area = PyObject_GC_New(DmStatsAreaObject, &DmStatsArea_Type);
    area->ob_area_id = area_id;
    area->ob_weakreflist = NULL;
    area->ob_stats = stats;

    /* We keep a reference on the parent DmStats to prevent it (and its handle)
     * from being deallocated.
     */
    Py_INCREF(stats);
    return area;
}

static int
DmStatsArea_init(DmStatsAreaObject *self, PyObject *args, PyObject *kwds)
{
    uint64_t area_id;
    if (!PyArg_ParseTuple(args, "i:__init__", &area_id))
        return -1;
    self->ob_area_id = area_id;
    return 0;
}

#define DMSTATSAREA___doc__ \
""

#define DMSTATSAREA__doc__ \
"Base class representing one area of a device-mapper statistics handle,\n"  \
"and the counter data contained within it."

static PyMethodDef DmStatsArea_methods[] = {
//    {"nr_areas", (PyCFunction)DmStatsArea_nr_areas, METH_NOARGS,
//        PyDoc_STR(DMSTATSREG_nr_areas__doc__)},
    {NULL, NULL}
};

static PyTypeObject DmStatsArea_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "dmpy.DmStatsArea",             /*tp_name*/
    sizeof(DmStatsAreaObject),      /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)DmStatsArea_dealloc,
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_reserved*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    DMSTATSAREA__doc__,         /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    offsetof(DmStatsAreaObject, ob_weakreflist), /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    DmStatsArea_methods,        /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    (initproc)DmStatsArea_init, /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};


/*
 * dmpy module methods.
 */

static PyObject *
_dmpy_get_lib_version(PyObject *self, PyObject *args)
{
    char version[64];
    if (!dm_get_library_version(version, sizeof(version))) {
        PyErr_SetString(PyExc_OSError, "Failed to get device-mapper "
                        "library version.");
        return NULL;
    } 
    return Py_BuildValue("s", version);
}

static PyObject *
_dmpy_update_nodes(PyObject *self, PyObject *args)
{
    dm_task_update_nodes();
    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
_dmpy_set_name_mangling_mode(PyObject *self, PyObject *args)
{
    int mangle_mode;

    if (!PyArg_ParseTuple(args, "i:set_name_mangling_mode", &mangle_mode))
        return NULL;

    if ((mangle_mode < 0) || (mangle_mode > DM_STRING_MANGLING_HEX)) {
        PyErr_SetString(PyExc_ValueError, "Name mangling mode value "
                        "out of range.");
        return NULL;
    }

    if (!dm_set_name_mangling_mode(mangle_mode)) {
        PyErr_SetString(PyExc_OSError, "Failed to set device-mapper name "
                        "mangling mode.");
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
_dmpy_get_name_mangling_mode(PyObject *self, PyObject *args)
{
    return Py_BuildValue("i", dm_get_name_mangling_mode());
}

static PyObject *
_dmpy_set_dev_dir(PyObject *self, PyObject *args)
{
    char *dir;

    if (!PyArg_ParseTuple(args, "s:set_dev_dir", &dir))
        goto fail;

    if (dir[0] != '/') {
        PyErr_Format(PyExc_ValueError, "Invalid directory value, %s: "
                     "not an absolute name.", dir);
        goto fail;
    }

    if (!dm_set_dev_dir(dir)) {
        PyErr_Format(PyExc_ValueError, "Invalid directory value, %s: "
                     "name too long.", dir);
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
_dmpy_get_dev_dir(PyObject *self, PyObject *args)
{
    return Py_BuildValue("s", dm_dir());
}

static PyObject *
_dmpy_set_sysfs_dir(PyObject *self, PyObject *args)
{
    char *dir;

    if (!PyArg_ParseTuple(args, "s:set_sysfs_dir", &dir))
        goto fail;

    if (dir[0] != '/') {
        PyErr_Format(PyExc_ValueError, "Invalid directory value, %s: "
                     "not an absolute name.", dir);
        goto fail;
    }

    if (!dm_set_sysfs_dir(dir)) {
        PyErr_Format(PyExc_ValueError, "Invalid directory value, %s: "
                     "name too long.", dir);
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
_dmpy_get_sysfs_dir(PyObject *self, PyObject *args)
{
    return Py_BuildValue("s", dm_sysfs_dir());
}

/* Taken from libdm/libdm-common.c */
#define DM_MAX_UUID_PREFIX_LEN  15

static PyObject *
_dmpy_set_uuid_prefix(PyObject *self, PyObject *args)
{
    char *prefix;

    if (!PyArg_ParseTuple(args, "s:set_uuid_prefix", &prefix))
        goto fail;

    if (!prefix) {
        PyErr_SetString(PyExc_ValueError, "New uuid prefix cannot be empty.");
        goto fail;
    }

    if (strlen(prefix) > DM_MAX_UUID_PREFIX_LEN) {
        PyErr_Format(PyExc_ValueError, "New uuid prefix %s too long.", prefix);
        goto fail;
    }

    if (!dm_set_uuid_prefix(prefix)) {
        PyErr_SetString(PyExc_OSError, "Failed to set new uuid prefix.");
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
_dmpy_get_uuid_prefix(PyObject *self, PyObject *args)
{
    return Py_BuildValue("s", dm_uuid_prefix());
}

static PyObject *
_dmpy_is_dm_major(PyObject *self, PyObject *args)
{
    int major;

    if (!PyArg_ParseTuple(args, "i:is_dm_major", &major))
        return NULL;

    if (dm_is_dm_major(major)) {
        Py_INCREF(Py_True);
        return Py_True;
    } else {
        Py_INCREF(Py_False);
        return Py_False;
    }
}

static PyObject *
_dmpy_lib_release(PyObject *self, PyObject *args)
{
    dm_lib_release();
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
_dmpy_hold_control_dev(PyObject *self, PyObject *args)
{
    int hold_open;
    PyObject *ret;

    if (!PyArg_ParseTuple(args, "i:hold_control_dev", &hold_open))
        return NULL;

    dm_hold_control_dev(hold_open);

    if (hold_open)
        ret = Py_True;
    else
        ret = Py_False;
    Py_INCREF(ret);

    return ret;
}

static PyObject *
_dmpy_mknodes(PyObject *self, PyObject *args)
{
    char *name = NULL;

    if (!PyArg_ParseTuple(args, "z:mknodes", &name))
        goto fail;

    if (!dm_mknodes(name)) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto fail;
    }

    Py_INCREF(Py_True);
    return Py_True;

fail:
    return NULL;
}

static PyObject *
_dmpy_driver_version(PyObject *self, PyObject *args)
{
    char version[DMPY_VERSION_BUF_LEN];

    if (!dm_driver_version(version, sizeof(version))) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    return Py_BuildValue("s", version);
}

static PyObject *
_dmpy_dump_memory(PyObject *self, PyObject *args)
{
    dm_dump_memory();
    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
_dmpy_udev_set_sync_support(PyObject *self, PyObject *args)
{
    int sync_with_udev;

    if (!PyArg_ParseTuple(args, "i:udev_set_sync_support", &sync_with_udev))
        return NULL;

    dm_udev_set_sync_support(sync_with_udev);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
_dmpy_udev_get_sync_support(PyObject *self, PyObject *args)
{
    return Py_BuildValue("i", dm_udev_get_sync_support());
}

static PyObject *
_dmpy_udev_set_checking(PyObject *self, PyObject *args)
{
    int checking;

    if (!PyArg_ParseTuple(args, "i:udev_set_checking", &checking))
        return NULL;

    dm_udev_set_checking(checking);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
_dmpy_udev_get_checking(PyObject *self, PyObject *args)
{
    return Py_BuildValue("i", dm_udev_get_checking());
}

static PyObject *
_dmpy_cookie_supported(PyObject *self, PyObject *args)
{
    PyObject *ret = Py_True;
    int cookie_supported = dm_cookie_supported();
    if (!cookie_supported)
        ret = Py_False;
    Py_INCREF(ret);
    return ret;
}

static PyObject *
_dmpy_udev_create_cookie(PyObject *self, PyObject *args)
{
    DmCookieObject *cookie;
    uint32_t cookie_val;

    if (!dm_udev_create_cookie(&cookie_val)) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    cookie = PyObject_New(DmCookieObject, &DmCookie_Type);
    if (!cookie)
        return NULL;

    if (_DmCookie_init(cookie, cookie_val))
        return NULL;

    cookie->ob_cookie = cookie_val;
    _dmpy_set_cookie_values(cookie);
    return (PyObject *) cookie;
}

static PyObject *
_dmpy_udev_complete(PyObject *self, PyObject *args)
{
    DmCookieObject *cookie;

    if (!PyArg_ParseTuple(args, "O!", &DmCookie_Type, &cookie))
        return NULL;

    return DmCookie_udev_complete(cookie, NULL);
}

static PyObject *
_dmpy_udev_wait(PyObject *self, PyObject *args, PyObject *kwds)
{
    static char * kwlist[] = {"immediate", NULL};
    DmCookieObject *cookie;
    int immediate = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!|i", kwlist,
                                     &DmCookie_Type, &cookie, &immediate))
        return NULL;

    return _DmCookie_udev_wait(cookie, immediate);
}

static PyObject *
_dmpy_message_supports_precise_timestamps(PyObject *self, PyObject *args)
{
    int precise = dm_message_supports_precise_timestamps();
    PyObject *ret;

    if (precise)
        ret = Py_True;
    else
        ret = Py_False;

    Py_INCREF(ret);
    return ret;
}

static PyObject *
_dmpy_stats_driver_supports_precise(PyObject *self, PyObject *args)
{
    int precise = dm_stats_driver_supports_precise();
    PyObject *ret;

    if (precise)
        ret = Py_True;
    else
        ret = Py_False;

    Py_INCREF(ret);
    return ret;
}

static PyObject *
_dmpy_stats_driver_supports_histogram(PyObject *self, PyObject *args)
{
    int histogram = dm_stats_driver_supports_histogram();
    PyObject *ret;

    if (histogram)
        ret = Py_True;
    else
        ret = Py_False;

    Py_INCREF(ret);
    return ret;
}

/* List of functions defined in the module */

#define DMPY_get_library_version__doc__ "Get the version of the device-mapper" \
" library in use. Returns a string, for e.g. \"1.02.122 (2016-04-09)\"."

#define DMPY_update_nodes__doc__ \
"Call this to make or remove the device nodes associated with previously " \
"issued commands."

#define DMPY_set_name_mangling_mode__doc__ \
"Set mangling mode used for device-mapper names and uuids."

#define DMPY_get_name_mangling_mode__doc__ \
"Get mangling mode used for device-mapper names and uuids."

#define DMPY_set_dev_dir__doc__ \
"Configure the device-mapper directory."

#define DMPY_get_dev_dir__doc__ \
"Return the configured device-mapper directory."

#define DMPY_set_sysfs_dir__doc__ \
"Configure the sysfs directory."

#define DMPY_get_sysfs_dir__doc__ \
"Return the configured sysfs directory."

#define DMPY_set_uuid_prefix__doc__ \
"Configure default UUID prefix string. Conventionally this is a short "      \
"capitalised prefix indicating the subsystem that is managing the devices, " \
"e.g. \"LVM-\" or \"MPATH-\".\n\n"                                           \
"To support stacks of devices from different subsystems, recursive "         \
"functions stop recursing if they reach a device with a different prefix."   \

#define DMPY_get_uuid_prefix__doc__ \
"Return the currently configured UUID prefix string."

#define DMPY_is_dm_major__doc__ \
"Returns `True` if the given major number belongs to device-mapper, or " \
"`False` otherwise."

#define DMPY_lib_release__doc__ \
"Release libdevmapper library resources and close the control file " \
"descriptor (unless `hold_control_dev` has been set)."

#define DMPY_hold_control_dev__doc__ \
"Enable holding the device-mapper control device open between ioctls:\n" \
"an optimisation for clients making repeated dm-ioctl calls."

#define DMPY_mknodes__doc__ \
"Create device nodes for the given device, or all devices, if none is given."

#define DMPY_driver_version__doc__ \
"Return a string description of the current device-mapper driver version."

#define DMPY_dump_memory__doc__ \
"Call dm_dump_memory() to dump memory debugging information to the log."

#define DMPY_udev_set_sync_support__doc__ \
"Enable or disable udev synchronization for the dmpy module."

#define DMPY_udev_get_sync_support__doc__ \
"Return the status of udev synchronization for the dmpy module."

#define DMPY_udev_set_checking__doc__ \
"Enable or disable additional udev synchronization checks for the dmpy module."

#define DMPY_udev_get_checking__doc__ \
"Return the status of udev synchronization checks for the dmpy module."

#define DMPY_cookie_supported__doc__ \
"Return True if device-mapper supports udev synchronization cookies, " \
"or False otherwise."

#define DMPY_udev_create_cookie__doc__ \
"Create a new device-mapper UDEV cookie for use in later operations."

#define DMPY_udev_complete__doc__ \
"Complete the UDEV transaction for this cookie."

#define DMPY_udev_wait__doc__ \
"Wait until the transaction represented by `cookie` is ready.\n\n"       \
"Returns `True` if the wait is successful and the transaction is\n"      \
"complete.\n\n"                                                          \
"Following a successful wait the `DmCookie.ready` member is set to\n"    \
"`True`.\n\n"                                                            \
"On error an OSError exception is raised.\n\n"                           \
"Unless `immediate=True` and the transaction is not ready, the cookie\n" \
"resources are released and no further calls to `udev_wait()`,\n"        \
"or `udev_complete() should be made."

#define DMPY_message_supports_precise_timestamps__doc__ \
"Test for the presence of precise_timestamps and histogram support.\n"   \
"Returns True if the running kernel supports the feature, or False\n"    \
"otherwise."

#define DMPY_stats_driver_supports_precise__doc__ \
"Test for the presence of precise_timestamps support.\n"                 \
"Returns True if the running kernel supports the feature, or False\n"    \
"otherwise."

#define DMPY_stats_driver_supports_histogram__doc__ \
"Test for the presence of histogram support.\n"                          \
"Returns True if the running kernel supports the feature, or False\n"    \
"otherwise."

#define DMPY___doc__ \
""

static PyMethodDef dmpy_methods[] = {
    {"get_library_version", (PyCFunction)_dmpy_get_lib_version, METH_NOARGS,
        PyDoc_STR(DMPY_get_library_version__doc__)},
    {"update_nodes", (PyCFunction)_dmpy_update_nodes, METH_NOARGS,
        PyDoc_STR(DMPY_update_nodes__doc__)},
    {"set_name_mangling_mode", (PyCFunction)_dmpy_set_name_mangling_mode,
        METH_VARARGS, PyDoc_STR(DMPY_set_name_mangling_mode__doc__)},
    {"get_name_mangling_mode", (PyCFunction)_dmpy_get_name_mangling_mode,
        METH_NOARGS, PyDoc_STR(DMPY_get_name_mangling_mode__doc__)},
    {"set_dev_dir", (PyCFunction)_dmpy_set_dev_dir, METH_VARARGS,
        PyDoc_STR(DMPY_set_dev_dir__doc__)},
    {"get_dev_dir", (PyCFunction)_dmpy_get_dev_dir, METH_NOARGS,
        PyDoc_STR(DMPY_get_dev_dir__doc__)},
    {"set_sysfs_dir", (PyCFunction)_dmpy_set_sysfs_dir, METH_VARARGS,
        PyDoc_STR(DMPY_set_sysfs_dir__doc__)},
    {"get_sysfs_dir", (PyCFunction)_dmpy_get_sysfs_dir, METH_NOARGS,
        PyDoc_STR(DMPY_get_sysfs_dir__doc__)},
    {"set_uuid_prefix", (PyCFunction)_dmpy_set_uuid_prefix, METH_VARARGS,
        PyDoc_STR(DMPY_set_uuid_prefix__doc__)},
    {"get_uuid_prefix", (PyCFunction)_dmpy_get_uuid_prefix, METH_NOARGS,
        PyDoc_STR(DMPY_get_uuid_prefix__doc__)},
    {"is_dm_major", (PyCFunction)_dmpy_is_dm_major, METH_VARARGS,
        PyDoc_STR(DMPY_is_dm_major__doc__)},
    {"lib_release", (PyCFunction)_dmpy_lib_release, METH_NOARGS,
        PyDoc_STR(DMPY_lib_release__doc__)},
    {"hold_control_dev", (PyCFunction)_dmpy_hold_control_dev, METH_VARARGS,
        PyDoc_STR(DMPY_hold_control_dev__doc__)},
    {"mknodes", (PyCFunction)_dmpy_mknodes, METH_VARARGS,
        PyDoc_STR(DMPY_mknodes__doc__)},
    {"driver_version", (PyCFunction)_dmpy_driver_version, METH_NOARGS,
        PyDoc_STR(DMPY_driver_version__doc__)},
    {"dump_memory", (PyCFunction)_dmpy_dump_memory, METH_NOARGS,
        PyDoc_STR(DMPY_dump_memory__doc__)},
    {"udev_set_sync_support", (PyCFunction)_dmpy_udev_set_sync_support,
        METH_VARARGS, PyDoc_STR(DMPY_udev_set_sync_support__doc__)},
    {"udev_get_sync_support", (PyCFunction)_dmpy_udev_get_sync_support,
        METH_NOARGS, PyDoc_STR(DMPY_udev_get_sync_support__doc__)},
    {"udev_set_checking", (PyCFunction)_dmpy_udev_set_checking,
        METH_VARARGS, PyDoc_STR(DMPY_udev_set_checking__doc__)},
    {"udev_get_checking", (PyCFunction)_dmpy_udev_get_checking,
        METH_NOARGS, PyDoc_STR(DMPY_udev_get_checking__doc__)},
    {"cookie_supported", (PyCFunction)_dmpy_cookie_supported,
        METH_NOARGS, PyDoc_STR(DMPY_cookie_supported__doc__)},
    {"udev_create_cookie", (PyCFunction)_dmpy_udev_create_cookie,
        METH_NOARGS, PyDoc_STR(DMPY_udev_create_cookie__doc__)},
    {"udev_complete", (PyCFunction)_dmpy_udev_complete,
        METH_VARARGS, PyDoc_STR(DMPY_udev_complete__doc__)},
    {"udev_wait", (PyCFunction)_dmpy_udev_wait,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(DMPY_udev_wait__doc__)},
    {"message_supports_precise_timestamps",
        (PyCFunction)_dmpy_message_supports_precise_timestamps,
        METH_NOARGS,
        PyDoc_STR(DMPY_message_supports_precise_timestamps__doc__)},
    {"stats_driver_supports_precise",
        (PyCFunction)_dmpy_stats_driver_supports_precise,
        METH_NOARGS, PyDoc_STR(DMPY_stats_driver_supports_precise__doc__)},
    {"stats_driver_supports_histogram",
        (PyCFunction)_dmpy_stats_driver_supports_histogram,
        METH_NOARGS, PyDoc_STR(DMPY_stats_driver_supports_histogram__doc__)},
    {NULL, NULL}           /* sentinel */
};

/* Device-mapper UDEV flags */
static const uint32_t _dmpy_udev_flags[] = {
    DM_UDEV_DISABLE_DM_RULES_FLAG,
    DM_UDEV_DISABLE_SUBSYSTEM_RULES_FLAG,
    DM_UDEV_DISABLE_DISK_RULES_FLAG,
    DM_UDEV_DISABLE_OTHER_RULES_FLAG,
    DM_UDEV_LOW_PRIORITY_FLAG,
    DM_UDEV_DISABLE_LIBRARY_FALLBACK,
    DM_UDEV_PRIMARY_SOURCE_FLAG,
    DM_SUBSYSTEM_UDEV_FLAG0,
    DM_SUBSYSTEM_UDEV_FLAG1,
    DM_SUBSYSTEM_UDEV_FLAG2,
    DM_SUBSYSTEM_UDEV_FLAG3,
    DM_SUBSYSTEM_UDEV_FLAG4,
    DM_SUBSYSTEM_UDEV_FLAG5,
    DM_SUBSYSTEM_UDEV_FLAG6,
    DM_SUBSYSTEM_UDEV_FLAG7,
    0
};

/* Order matches _dmpy_udev_flags */
static const char *_dmpy_udev_flag_names[] = {
    "UDEV_DISABLE_DM_RULES_FLAG",
    "UDEV_DISABLE_SUBSYSTEM_RULES_FLAG",
    "UDEV_DISABLE_DISK_RULES_FLAG",
    "UDEV_DISABLE_OTHER_RULES_FLAG",
    "UDEV_LOW_PRIORITY_FLAG",
    "UDEV_DISABLE_LIBRARY_FALLBACK",
    "UDEV_PRIMARY_SOURCE_FLAG",
    "SUBSYSTEM_UDEV_FLAG0",
    "SUBSYSTEM_UDEV_FLAG1",
    "SUBSYSTEM_UDEV_FLAG2",
    "SUBSYSTEM_UDEV_FLAG3",
    "SUBSYSTEM_UDEV_FLAG4",
    "SUBSYSTEM_UDEV_FLAG5",
    "SUBSYSTEM_UDEV_FLAG6",
    "SUBSYSTEM_UDEV_FLAG7",
    NULL
};

/*
 * Add module variables for DM_STATS_* constants.
 */
static int _dmpy_add_stats_constants(PyObject *m)
{
    PyModule_AddObject(m, "STATS_ALL_PROGRAMS",
                       Py_BuildValue("s", DM_STATS_ALL_PROGRAMS));
    PyModule_AddObject(m, "STATS_REGIONS_ALL",
                       Py_BuildValue("l", DM_STATS_REGIONS_ALL));
    return 0;
}

/*
 * Add module variables for DM_UDEV_* and DM_SUBSYSTEM_UDEV_* flags.
 */
static int _dmpy_add_udev_flags(PyObject *m)
{
    const uint32_t *flag = _dmpy_udev_flags;
    const char **flag_name = &_dmpy_udev_flag_names[0];
    PyObject *flag_value;

    while(*flag && *flag_name) {
        flag_value = PyLong_FromLong((long) *flag);
        if (!flag_value)
            return -1;
        PyModule_AddObject(m, *flag_name, flag_value);
        flag_name++;
        flag++;
    }
    return 0;
}

/* Add module variables for DM_READ_AHEAD_* constants.
 */
static int _dmpy_add_read_ahead_types(PyObject *m)
{
    PyObject *v = NULL;

    /* DM_READ_AHEAD_AUTO */
    v = PyLong_FromLong((long) DM_READ_AHEAD_AUTO);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "READ_AHEAD_AUTO", v);

    /* DM_READ_AHEAD_NONE */
    v = PyLong_FromLong((long) DM_READ_AHEAD_NONE);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "READ_AHEAD_NONE", v);

    /* DM_READ_AHEAD_MINIMUM_FLAG */
    v = PyLong_FromLong((long) DM_READ_AHEAD_MINIMUM_FLAG);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "READ_AHEAD_MINIMUM_FLAG", v);

    return 0;

fail:
    return -1;
}

/* Add module variables for dm_string_mangling_t enum.
 */

static int
_dmpy_add_string_mangling_types(PyObject *m)
{
    PyObject *v = NULL;

    /* DM_STRING_MANGLING_NONE */
    v = PyLong_FromLong((long) DM_STRING_MANGLING_NONE);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "STRING_MANGLING_NONE", v);

    /* DM_STRING_MANGLING_AUTO */
    v = PyLong_FromLong((long) DM_STRING_MANGLING_AUTO);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "STRING_MANGLING_AUTO", v);

    /* DM_STRING_MANGLING_HEX */
    v = PyLong_FromLong((long) DM_STRING_MANGLING_HEX);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "STRING_MANGLING_HEX", v);

    return 0;

fail:
    return -1;
}

/* Add module variables for dm_add_node_t enum.
 */
static int
_dmpy_add_add_node_types(PyObject *m)
{
    PyObject *v = NULL;

    /* DM_ADD_NODE_ON_RESUME */
    v = PyLong_FromLong((long) DM_ADD_NODE_ON_RESUME);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "ADD_NODE_ON_RESUME", v);

    /* DM_ADD_NODE_ON_CREATE */
    v = PyLong_FromLong((long) DM_ADD_NODE_ON_CREATE);
    if (!v)
        goto fail;
    Py_INCREF(v);
    PyModule_AddObject(m, "ADD_NODE_ON_CREATE", v);

    return 0;

fail:
    return -1;
}

/* Add module variables for device-mapper task types (DM_DEVICE_...).
 */
static int
_dmpy_add_task_types(PyObject *m)
{
    PyObject *v = NULL;
    int i = 0;

    for (i = DM_DEVICE_CREATE; i <= DM_DEVICE_SET_GEOMETRY; i++) {
        v = PyLong_FromLong((long) i);
    	if (!v)
            goto fail;
        Py_INCREF(v);
        PyModule_AddObject(m, _dm_task_type_names[i], v);
    }

    return 0;

    fail:
    	return -1;    
}

static int
dmpy_exec(PyObject *m)
{
    /* initialise dm globals */
    dm_lib_init();

    /* Register AtExit call to dm_lib_exit() */
    if (Py_AtExit(dm_lib_exit) < 0)
        goto fail;

    /* Due to cross platform compiler issues the slots must be filled
     * here. It's required for portability to Windows without requiring
     * C++. */
    DmTimestamp_Type.tp_base = &PyBaseObject_Type;
    DmTimestamp_Type.tp_new = &PyType_GenericNew;

    DmCookie_Type.tp_base = &PyBaseObject_Type;
    DmCookie_Type.tp_new = &PyType_GenericNew;

    DmInfo_Type.tp_base = &PyBaseObject_Type;
    DmInfo_Type.tp_new = PyType_GenericNew;

    DmTask_Type.tp_base = &PyBaseObject_Type;
    DmTask_Type.tp_new = PyType_GenericNew;

    DmStats_Type.tp_base = &PyBaseObject_Type;
    DmStats_Type.tp_new = PyType_GenericNew;

    DmStatsRegion_Type.tp_base = &PyBaseObject_Type;
    DmStatsRegion_Type.tp_new = PyType_GenericNew;

    if (PyType_Ready(&DmTimestamp_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmCookie_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmInfo_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmTask_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmStats_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmStatsRegion_Type) < 0)
        goto fail;

    PyModule_AddObject(m, "DmStats", (PyObject *) &DmStats_Type);
    PyModule_AddObject(m, "DmTask", (PyObject *) &DmTask_Type);
    PyModule_AddObject(m, "DmCookie", (PyObject *) &DmCookie_Type);
    PyModule_AddObject(m, "DmTimestamp", (PyObject *) &DmTimestamp_Type);

    /* Add some symbolic constants to the module */
    if (DmErrorObject == NULL) {
        DmErrorObject = PyErr_NewException("dmpy.DmError", NULL, NULL);
        if (DmErrorObject == NULL)
            goto fail;
    }
    Py_INCREF(DmErrorObject);
    PyModule_AddObject(m, "DmError", DmErrorObject);

    if (_dmpy_add_string_mangling_types(m))
        goto fail;

    if (_dmpy_add_task_types(m))
        goto fail;

    if (_dmpy_add_add_node_types(m))
        goto fail;

    if (_dmpy_add_read_ahead_types(m))
        goto fail;

    if (_dmpy_add_udev_flags(m))
        goto fail;

    if (_dmpy_add_stats_constants(m))
        goto fail;

    return 0;
 fail:
    Py_XDECREF(m);
    return -1;
}

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef_Slot dmpy_slots[] = {
    {Py_mod_exec, dmpy_exec},
    {0, NULL},
};

static struct PyModuleDef dmpymodule = {
    PyModuleDef_HEAD_INIT,
    "dmpy",
    dmpy__doc__,
    0,
    dmpy_methods,
    dmpy_slots,
    NULL,
    NULL,
    NULL
};
#endif /* PY_MAJOR_VERSION >= 3 */

#if PY_MAJOR_VERSION >= 3
/* Export function for the py3 module (*must* be called PyInit_dmpy) */
PyMODINIT_FUNC
PyInit_dmpy(void)
{
    return PyModuleDef_Init(&dmpymodule);
}
#else
/* Export function for the py2 module (*must* be called initdmpy) */
PyMODINIT_FUNC
initdmpy(void)
{
    PyObject *m;
    m = Py_InitModule3("dmpy", dmpy_methods, dmpy__doc__);
    return (void) dmpy_exec(m);
}
#endif /* PY_MAJOR_VERSION >= 3 */
/* # vim: set et ts=4 sw=4 : */
