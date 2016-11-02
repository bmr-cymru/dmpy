/*
 * dmpy - Python device-mapper interface.
 *
 * Copyright (C) 2016 Red Hat, Inc., Bryn M. Reeves <bmr@redhat.com>
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

#ifdef __linux__
    #include "linux/kdev_t.h"
#else
    #define MAJOR(x) major((x))
    #define MINOR(x) minor((x))
#endif

#define DMPY_DEBUG 1

#ifdef DMPY_DEBUG
    #define dmpy_debug(x...) fprintf(stderr, x)
#else
    #define dmpy_debug(x...)
#endif

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

#define DMTASK_copy__doc__ \
"Copy this DmTimestamp."

#define DMTASK_get__doc__ \
"Update this DmTimestamp to the current time."

#define DMTASK_compare__doc__ \
"Compare two DmTimestamp objects."

#define DMTASK_delta__doc__ \
"Return the absolute difference, in nanoseconds, between two " \
"DmTimestamp objects."

static PyMethodDef DmTimestamp_methods[] = {
    {"copy", (PyCFunction)DmTimestamp_copy, METH_VARARGS,
        PyDoc_STR(DMTASK_copy__doc__)},
    {"get", (PyCFunction)DmTimestamp_get, METH_NOARGS,
        PyDoc_STR(DMTASK_get__doc__)},
    {"compare", (PyCFunction)DmTimestamp_compare, METH_VARARGS,
        PyDoc_STR(DMTASK_compare__doc__)},
    {"delta", (PyCFunction)DmTimestamp_delta, METH_VARARGS,
        PyDoc_STR(DMTASK_delta__doc__)},
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
    uint32_t ob_value;
    uint16_t ob_val_prefix;
    uint16_t ob_val_base;
} DmCookieObject;

static PyTypeObject DmCookie_Type;

#define DmCookieObject_Check(v)      (Py_TYPE(v) == &DmCookie_Type)

static void
_dmpy_set_cookie_values(DmCookieObject *self)
{
    self->ob_val_base = self->ob_value & ~DM_UDEV_FLAGS_MASK;
    self->ob_val_prefix = ((self->ob_value & DM_UDEV_FLAGS_MASK)
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

static int
DmCookie_init(DmCookieObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"value", NULL};
    int value = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:__init__",
                                     kwlist, &value))
        return -1;

    if (_dmpy_check_cookie_value(value))
        return -1;

    self->ob_value = (uint32_t) value;
    _dmpy_set_cookie_values(self);
    return 0;
}

static PyObject *
DmCookie_set_value(DmCookieObject *self, PyObject *args)
{
    int value;

    if (!PyArg_ParseTuple(args, "i:set_value", &value))
        return NULL;

    if (!_dmpy_check_cookie_value(value))
        return NULL;

    self->ob_value = (uint32_t) value;
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

    self->ob_value = (uint32_t) (self->ob_value & ~DM_UDEV_FLAGS_MASK);
    self->ob_value |= prefix;
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

    self->ob_value = (uint32_t) (self->ob_value & DM_UDEV_FLAGS_MASK);
    self->ob_value |= base;
    _dmpy_set_cookie_values(self);

    Py_INCREF(Py_True);
    return Py_True;
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

static PyMethodDef DmCookie_methods[] = {
    {"set_value", (PyCFunction)DmCookie_set_value, METH_VARARGS,
        PyDoc_STR(DMCOOKIE_set_value__doc__)},
    {"set_prefix", (PyCFunction)DmCookie_set_prefix, METH_VARARGS,
        PyDoc_STR(DMCOOKIE_set_value__doc__)},
    {"set_base", (PyCFunction)DmCookie_set_base, METH_VARARGS,
        PyDoc_STR(DMCOOKIE_set_value__doc__)},
    {NULL, NULL}
};

/* Attributes for DmCookieObject struct members.
 */
static PyMemberDef DmCookie_members[] = {
    {"value", T_INT, offsetof(DmCookieObject, ob_value), READONLY,
     PyDoc_STR("The current value of this `DmCookie`.")},
    {"prefix", T_SHORT, offsetof(DmCookieObject, ob_val_prefix), READONLY,
     PyDoc_STR("The current prefix value of this `DmCookie`.")},
    {"base", T_SHORT, offsetof(DmCookieObject, ob_val_base), READONLY,
     PyDoc_STR("The current base value of this `DmCookie`.")},
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
newDmInfoObject(PyObject *arg)
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


typedef struct {
    PyObject_HEAD
    struct dm_task *ob_dmt;
    DmCookieObject *cookie;
} DmTaskObject;

static PyTypeObject DmTask_Type;

#define DmTaskObject_Check(v)      (Py_TYPE(v) == &DmTask_Type)

static int
DmTask_init(DmTaskObject *self, PyObject *args, PyObject *kwds)
{
    int type;

    if (!PyArg_ParseTuple(args, "i:__init__", &type))
        return -1;

    self->cookie = NULL;

    if (!(self->ob_dmt = dm_task_create(type))) {
        /* FIXME: use dm_task_get_errno */
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

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

    Py_XDECREF(self->cookie);

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
    if (!(dm_task_run(self->ob_dmt))) {
        /* FIXME: use dm_task_get_errno */
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *DmTask_get_driver_version(DmTaskObject *self, PyObject *args)
{
    char version[DMPY_VERSION_BUF_LEN];
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

    info = newDmInfoObject(Py_None);
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
                        "from device-mapper");
        return NULL;
    }

    if (!(deps_list = PyList_New(0)))
        return NULL;

    for (i = 0; i < deps->count; i++) {
        dev = deps->device[i];
        value = Py_BuildValue("(ii)", MAJOR(dev), MINOR(dev));
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

    if (!(deps = dm_task_get_deps(self->ob_dmt))) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return _dm_build_deps_list(deps);
}

static PyObject *
_dm_build_versions_list(struct dm_versions *target)
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

    versions = dm_task_get_versions(self->ob_dmt);
    if (!versions) {
        PyErr_SetString(PyExc_OSError, "Failed to get task versions "
                        "from device-mapper");
        return NULL;
    }
    return _dm_build_versions_list(versions);
}

static PyObject *
DmTask_get_message_response(DmTaskObject *self, PyObject *args)
{
    return Py_BuildValue("s", dm_task_get_message_response(self->ob_dmt));
}

static PyObject *
DmTask_get_name(DmTaskObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"mangled", NULL};
    int mangled = -1; /* use name_mangling_mode */
    const char *name;

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
_dm_build_name_list(struct dm_names *names)
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

    if (!(names = dm_task_get_names(self->ob_dmt))) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return _dm_build_name_list(names);
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
        PyErr_Format(PyExc_ValueError, "Non empty new name is required.",
                     newname);
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
    self->cookie = cookie;

    if (!dm_task_set_cookie(self->ob_dmt, &cookie->ob_value, flags)) {
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

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
DmTask_get_ioctl_timestamp(DmTaskObject *self, PyObject *args)
{
    struct dm_timestamp *ts;
    DmTimestampObject *new_ts;

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

#define DMTASK_get_uuid__doc__                                          \
"Get the dm device's UUID based on the value of the mangling mode set " \
"during preceding dm_task_run call.\n. To force either mangled or "     \
"unmangled output, set the `mangle` kwarg to '1' or '0' respectively."

#define DMTASK_get_deps__doc__ \
"Get the list of dependencies for the dm device."

#define DMTASK_get_versions__doc__ \
"Get the library version in numerical format."

#define DMTASK_get_message_response__doc__ \
"Retrieve the response to a prior `DM_DEVICE_TARGET_MSG` as a string."

#define DMTASK_get_name__doc__ \
"Get the device name associated with this `DmTask`."

#define DMTASK_get_names__doc__ \
"Get the list of device names returned by a prior DM_DEVICE_LIST operation " \
"on this `DmTask`.\n\nReturns a list of 3-tuples containing the device " \
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
    {"get_uuid", (PyCFunction)DmTask_get_uuid, METH_NOARGS | METH_KEYWORDS,
        PyDoc_STR(DMTASK_get_uuid__doc__)},
    {"get_deps", (PyCFunction)DmTask_get_deps, METH_NOARGS,
        PyDoc_STR(DMTASK_get_deps__doc__)},
    {"get_versions", (PyCFunction)DmTask_get_versions, METH_NOARGS,
        PyDoc_STR(DMTASK_get_deps__doc__)},
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

#define DMPY_hold_control_dev__doc__ \
"Enable holding the device-mapper control device open between ioctls:\n" \
"an optimisation for clients making repeated dm-ioctl calls."

#define DMPY_mknodes__doc__ \
"Create device nodes for the given device, or all devices, if none is given."

#define DMPY_driver_version__doc__ \
"Return a string description of the current device-mapper driver version."

#define DMPY_dump_memory__doc__ \
"Call dm_dump_memory() to dump memory debugging information to the log."

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
    {"hold_control_dev", (PyCFunction)_dmpy_hold_control_dev, METH_VARARGS,
        PyDoc_STR(DMPY_hold_control_dev__doc__)},
    {"mknodes", (PyCFunction)_dmpy_mknodes, METH_VARARGS,
        PyDoc_STR(DMPY_mknodes__doc__)},
    {"driver_version", (PyCFunction)_dmpy_driver_version, METH_NOARGS,
        PyDoc_STR(DMPY_driver_version__doc__)},
    {"dump_memory", (PyCFunction)_dmpy_dump_memory, METH_NOARGS,
        PyDoc_STR(DMPY_dump_memory__doc__)},
    {NULL, NULL}           /* sentinel */
};

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

    if (PyType_Ready(&DmTimestamp_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmCookie_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmInfo_Type) < 0)
        goto fail;

    if (PyType_Ready(&DmTask_Type) < 0)
        goto fail;

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

    return 0;
 fail:
    Py_XDECREF(m);
    return -1;
}

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

/* Export function for the module (*must* be called PyInit_dmpy) */

PyMODINIT_FUNC
PyInit_dmpy(void)
{
    return PyModuleDef_Init(&dmpymodule);
}
