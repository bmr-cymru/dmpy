import unittest

DM_NAME_LEN=128  # includes NULL

def _get_dm_major_from_dm_0_sysfs():
    dm0_sysfs_path = "/sys/block/dm-0/dev"
    major = None
    minor = None
    with open(dm0_sysfs_path, "r") as f:
        (major, minor) = f.read().strip().split(":")
    return int(major)

def _get_dm_major_from_proc():
    proc_devices_path = "/proc/devices"
    with open(proc_devices_path) as f:
        block_seen = False
        for line in f.readlines():
            if "Block" in line:
                block_seen = True
                continue
            if block_seen and "device-mapper" in line:
                (major, name) = line.split(" ")
                break
    return int(major)

def _get_dm_major():
    try:
        return _get_dm_major_from_dm_0_sysfs()
    except:
        try:
            return _get_dm_major_from_proc()
        except:
            return 253


class DmpyTests(unittest.TestCase):

    def test_import(self):
        # attempt to import dmpy
        try:
            import dmpy as dm
        except Exception as e:
            self.fail(str(e))

    def test_dmpy_get_library_version(self):
        import dmpy as dm
        # Assert the expected major/minor version values (good since Nov 2005).
        libdm_major_minor = "1.02"
        self.assertTrue(dm.get_library_version().startswith(libdm_major_minor))

    def test_is_dm_major(self):
        import dmpy as dm
        # Assert that invalid dm major numbers return False.
        self.assertFalse(dm.is_dm_major(0))
        self.assertFalse(dm.is_dm_major(1))
        self.assertFalse(dm.is_dm_major(-1))
        # Assert that valid dm major numbers return True.
        self.assertTrue(dm.is_dm_major(_get_dm_major()))

    def test_dm_task_types_all_new(self):
        # test creation of each defined DM_DEVICE_* task type
        import dmpy as dm
        task_types = [
            dm.DM_DEVICE_CREATE,
            dm.DM_DEVICE_RELOAD,
            dm.DM_DEVICE_REMOVE,
            dm.DM_DEVICE_REMOVE_ALL,
            dm.DM_DEVICE_SUSPEND,
            dm.DM_DEVICE_RESUME,
            dm.DM_DEVICE_INFO,
            dm.DM_DEVICE_DEPS,
            dm.DM_DEVICE_RENAME,
            dm.DM_DEVICE_VERSION,
            dm.DM_DEVICE_STATUS,
            dm.DM_DEVICE_TABLE,
            dm.DM_DEVICE_WAITEVENT,
            dm.DM_DEVICE_LIST,
            dm.DM_DEVICE_CLEAR,
            dm.DM_DEVICE_MKNODES,
            dm.DM_DEVICE_LIST_VERSIONS,
            dm.DM_DEVICE_TARGET_MSG,
            dm.DM_DEVICE_SET_GEOMETRY
        ]
        for ttype in task_types:
            dmt = dm.DmTask(ttype)

    def _test_empty_task_method_raises(self, method):
        # Issue a DM_DEVICE_VERSION (which returns nothing but the driver
        # version), and then attempt to call `method`, which should be the
        # name of a `DmTask` method that raises `TypeError` when no data
        # is present.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_VERSION)
        dmt.run()
        runnable = getattr(dmt, method)
        self.assertTrue(runnable)
        self.assertRaises(TypeError, runnable)

    def test_empty_get_name_raises(self):
        # dm_task_get_name() returns the name from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return a
        # device name), then attempting to call dm_task_get_name() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        self._test_empty_task_method_raises("get_name")

    def test_empty_get_info_raises(self):
        # dm_task_get_info() returns the info from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return a
        # device info), then attempting to call dm_task_get_info() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        self._test_empty_task_method_raises("get_info")

    def test_empty_get_deps_raises(self):
        # dm_task_get_deps() returns the deps from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return
        # device deps), then attempting to call dm_task_get_deps() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        self._test_empty_task_method_raises("get_deps")

    def test_empty_get_uuid_raises(self):
        # dm_task_get_uuid() returns the uuid from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return
        # device uuid), then attempting to call dm_task_get_uuid() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        self._test_empty_task_method_raises("get_uuid")

    def test_empty_get_message_response_raises(self):
        # dm_task_get_message() returns the message from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return
        # device message), then attempting to call dm_task_get_message() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        self._test_empty_task_method_raises("get_message_response")

    def test_empty_get_names_raises(self):
        # dm_task_get_names() returns the names from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return
        # device names), then attempting to call dm_task_get_names() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        self._test_empty_task_method_raises("get_names")

    def test_get_name_list_and_check_types(self):
        # We don't really care what devices are present - just that we get
        # the expected list of 3-tuples with (str, int, int) types.
        #
        # Fail the test if no devices are found.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_LIST)
        dmt.run()
        names = dmt.get_names()
        self.assertTrue(len(names))
        for name in names:
            self.assertTrue(type(name[0]) == str)  # name
            self.assertTrue(type(name[1]) == int)  # major
            self.assertTrue(type(name[2]) == int)  # minor

    def test_set_newname_name_ok(self):
        # Assert that a valid newname can be set via DmTask.set_newname()
        # on a DmTask(DM_DEVICE_RENAME) task.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dm_name_ok = (DM_NAME_LEN - 1) * "A"
        self.assertTrue(dmt.set_newname(dm_name_ok))

    def test_set_newname_null_name(self):
        # Assert that TypeError is raised when name is NULL.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dm_name_null = None
        with self.assertRaises(TypeError) as cm:
            dmt.set_newname(dm_name_null)

    def test_set_newname_empty_name(self):
        # Assert that ValueError is raised when name is "".
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dm_name_empty = ""
        with self.assertRaises(ValueError) as cm:
            dmt.set_newname(dm_name_empty)

    def test_set_newname_name_has_slash(self):
        # Assert that ValueError is raised when name contains '/'
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dm_name_has_slash = "/qux"
        with self.assertRaises(ValueError) as cm:
            dmt.set_newname(dm_name_has_slash)

    def test_set_newname_name_too_long(self):
        # Assert that ValueError is raised when len(name) > (DM_NAME_LEN - 1).
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dm_name_too_long = DM_NAME_LEN * "A"
        with self.assertRaises(ValueError) as cm:
            dmt.set_newname(dm_name_too_long)

# vim: set et ts=4 sw=4 :
