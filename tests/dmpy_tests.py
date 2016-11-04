import unittest

class DmpyTests(unittest.TestCase):

    def test_import(self):
        # attempt to import dmpy
        try:
            import dmpy as dm
        except Exception as e:
            self.fail(str(e))

    def test_dm_task_types_new(self):
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

    def test_empty_get_name(self):
        # dm_task_get_name() returns the name from the results of an ioctl.
        # If no ioctl has been peformed (or if the ioctl did not return a
        # device name), then attempting to call dm_task_get_name() will
        # trigger a segmentation fault. The Python bindings need to detect
        # this and return the Py_None type.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_VERSION)
        dmt.run()
        self.assertRaises(TypeError, dmt.get_name)

