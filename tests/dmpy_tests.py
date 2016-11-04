import unittest

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
