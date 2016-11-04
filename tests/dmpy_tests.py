# Copyright (C) 2016 Red Hat, Inc. Bryn M. Reeves <bmr@redhat.com>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2, as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA

import unittest
import shlex
from os import readlink, unlink
from os.path import exists, join
from subprocess import Popen, PIPE, STDOUT

# Non-exported device-mapper constants: used for tests only.
DM_NAME_LEN = 128  # includes NULL
DM_MAX_UUID_PREFIX_LEN = 15


def _get_cmd_output(cmd):
    """ Call `cmd` via `Popen` and return the status and combined `stdout`
        and `stderr` as a 2-tuple, e.g.:

        (0, "vg00/lvol0: Created new region with 1 area(s) as region ID 5\n")

    """
    args = shlex.split(cmd)

    p = Popen(args, shell=False, stdout=PIPE,
              stderr=STDOUT, bufsize=-1, close_fds=True)
    # stderr will always be None
    (stdout, stderr) = p.communicate()

    # Change the codec if testing in a non-utf8 environment.
    return (p.returncode, stdout.decode('utf-8'))


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


# Try to find the (a) current device-mapper major number from sysfs,
# or /proc/devices. This is used to test dmpy.is_dm_major().
def _get_dm_major():
    try:
        return _get_dm_major_from_dm_0_sysfs()
    except:
        try:
            return _get_dm_major_from_proc()
        except:
            return 253


def _get_driver_version_from_dmsetup():
    for line in _get_cmd_output("dmsetup version")[1].splitlines():
        if not line.startswith("Driver version"):
            continue
        return line.split(":")[1].lstrip()


def _create_loopback(path, size):
    loop_file = join(path, "dmpy-test-img0")
    r = _get_cmd_output("dd if=/dev/zero of=%s bs=%d count=1" %
                        (loop_file, size))
    if r[0]:
        raise OSError("Failed to create image file.")
    r = _get_cmd_output("losetup -f")
    if r[0]:
        raise OSError("Failed to find free loop device.")
    device = r[1].strip()
    r = _get_cmd_output("losetup %s %s" % (device, loop_file))
    return (device, loop_file)


def _remove_loopback(loop_device):
    device, loop_file = loop_device
    r = _get_cmd_output("losetup -d %s" % device)
    if r[0]:
        raise OSError("Failed to remove loop device.")
    unlink(loop_file)


def _create_linear_device(dev, size):
    dm_name = "dmpytest0"
    sectors = size >> 9

    r = _get_cmd_output("dmsetup create %s --table='0 %d linear %s 0'" %
                        (dm_name, sectors, dev))
    if r[0]:
        raise OSError("Failed to create linear device.")

    return dm_name


def _remove_dm_device(dm_dev):
    r = _get_cmd_output("dmsetup remove %s" % dm_dev)
    if r[0]:
        raise OSError("Failed to create linear device.")


class DmpyTests(unittest.TestCase):

    test_dev_size_bytes = 2**20
    nodev = "quxquxquxquxqux"
    loop0 = None
    dmpytest0 = None

    def setUp(self):
        dev_size = self.test_dev_size_bytes
        self.loop0 = _create_loopback("/var/tmp/", dev_size)
        self.dmpytest0 = _create_linear_device(self.loop0[0], dev_size)

    def tearDown(self):
        if (self.dmpytest0):
            _remove_dm_device(self.dmpytest0)
        if (self.loop0):
            _remove_loopback(self.loop0)

    def test_import(self):
        # attempt to import dmpy
        try:
            import dmpy as dm
        except Exception as e:
            self.fail(str(e))

    #
    # Dmpy module tests.
    #

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

    def test_update_nodes(self):
        pass  # FIXME: test with fake /dev and udev disabled.

    def test_set_get_name_mangling_mode(self):
        # Ensure that we get the same name_mangling_mode back as we set, and
        # that the default mode is as expected.
        import dmpy as dm
        # Assert that dm.STRING_MANGLING_AUTO is default.
        initial_mode = dm.get_name_mangling_mode()
        self.assertTrue(initial_mode == dm.STRING_MANGLING_AUTO)
        # Assert that we get each mangline mode back as expected.
        self.assertTrue(dm.set_name_mangling_mode(dm.STRING_MANGLING_NONE))
        self.assertTrue(dm.get_name_mangling_mode() == dm.STRING_MANGLING_NONE)
        self.assertTrue(dm.set_name_mangling_mode(dm.STRING_MANGLING_AUTO))
        self.assertTrue(dm.get_name_mangling_mode() == dm.STRING_MANGLING_AUTO)
        self.assertTrue(dm.set_name_mangling_mode(dm.STRING_MANGLING_HEX))
        self.assertTrue(dm.get_name_mangling_mode() == dm.STRING_MANGLING_HEX)

    def test_set_get_dev_dir(self):
        # Ensure that we get the same dev_dir back as we set, and
        # that the default directory is as expected.
        import dmpy as dm
        # Default device directory. If your libdevmapper was compiled with
        # a different value, change the definition of `default_dev_dir_get`.
        default_dev_dir_get = "/dev/mapper"
        other_dev_dir_get = "/var/tmp/mapper"
        other_dev_dir = "/var/tmp"
        reset_dev_dir = "/dev"
        # Assert that dm.STRING_MANGLING_AUTO is default.
        initial_dev_dir = dm.get_dev_dir()
        # Assert that the expected string is returned following a set.
        self.assertTrue(initial_dev_dir == default_dev_dir_get)
        self.assertTrue(dm.set_dev_dir(other_dev_dir))
        self.assertTrue(dm.get_dev_dir() == other_dev_dir_get)
        self.assertTrue(dm.set_dev_dir(reset_dev_dir))
        self.assertTrue(dm.get_dev_dir() == default_dev_dir_get)

    def test_set_get_sysfs_dir(self):
        # Ensure that we get the same dev_dir back as we set, and
        # that the default directory is as expected.
        import dmpy as dm
        # Default device directory. If your libdevmapper was compiled with
        # a different value, change the definition of `default_dev_dir_get`.
        default_sysfs_dir_get = "/sys/"
        # Assert that default_sysfs_dir_get is default.
        initial_sysfs_dir = dm.get_sysfs_dir()
        # Assert that the expected string is returned following a set.
        self.assertTrue(initial_sysfs_dir == default_sysfs_dir_get)

    def test_set_sysfs_dir_non_abs_path(self):
        # Assert that attampting to set a non-absolute sysfs path raises a
        # TypeError exception.
        import dmpy as dm
        with self.assertRaises(ValueError) as cm:
            dm.set_sysfs_dir("./tests/sys")

    def test_set_uuid_prefix_too_long(self):
        # Assert that dmpy.set_uuid_prefix() with a prefix length
        # > DM_MAX_UUID_PREFIX_LEN raises ValueError.
        import dmpy as dm
        with self.assertRaises(ValueError) as cm:
            dm.set_uuid_prefix("X" * (DM_MAX_UUID_PREFIX_LEN + 1))

    def test_set_get_uuid_prefix(self):
        # Assert that the expected prefix is returned following a set,
        # and that the default is `LVM-`.
        # FIXME: verify that the prefix is used in commands.
        import dmpy as dm
        default_uuid_prefix = "LVM-"
        new_uuid_prefix = "QUX-"
        self.assertTrue(dm.get_uuid_prefix() == default_uuid_prefix)
        self.assertTrue(dm.set_uuid_prefix(new_uuid_prefix))
        self.assertTrue(dm.get_uuid_prefix() == new_uuid_prefix)

    def test_lib_release_releases_fd(self):
        # Assert that a call to dmpy.lib_release() closes the ioctl file
        # descriptor.
        import dmpy as dm
        dev_mapper_control = "/dev/mapper/control"
        control_fd_path = "/proc/self/fd/3"

        # Run a DM_DEVICE_LIST to open the ioctl fd.
        dmt = dm.DmTask(dm.DM_DEVICE_LIST)
        dmt.run()
        dmt = None
        # Control fd path should exist now.
        self.assertTrue(exists(control_fd_path))
        self.assertTrue(dev_mapper_control == readlink(control_fd_path))
        # Control fd path should be close following lib_release().
        dm.lib_release()
        self.assertFalse(exists(control_fd_path))

    def test_hold_control_dev_open(self):
        # Assert that dmpy.hold_control_dev_open() returns True.
        import dmpy as dm
        dev_mapper_control = "/dev/mapper/control"
        control_fd_path = "/proc/self/fd/3"

        # Enable holding the control fd.
        dm.hold_control_dev(1)
        # Run a DM_DEVICE_LIST to open the ioctl fd.
        dmt = dm.DmTask(dm.DM_DEVICE_LIST)
        dmt.run()
        dmt = None
        # Control fd path should exist now.
        self.assertTrue(exists(control_fd_path))
        self.assertTrue(dev_mapper_control == readlink(control_fd_path))
        dm.lib_release()
        # Control fd path should exist now.
        self.assertTrue(exists(control_fd_path))
        dm.hold_control_dev(0)
        dm.lib_release()
        # Control fd path should not exist now.
        self.assertFalse(exists(control_fd_path))

    def test_mknodes(self):
        pass  # FIXME: test with fake /dev and udev disabled.

    def test_driver_version(self):
        # Assert that the driver version string returned by
        # `dmpy.driver_version()` matches the one returned by dmsetup.
        import dmpy as dm
        dmpy_drv_version = dm.driver_version()
        dmsetup_drv_version = _get_driver_version_from_dmsetup()
        self.assertTrue(dmpy_drv_version == dmsetup_drv_version)

    def test_dump_memory(self):
        # FIXME: test with custom logging fn?
        pass

    #
    # DmTask tests.
    #

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
        # dm_task_get_message() returns the message from the results of an
        # ioctl. If no ioctl has been peformed (or if the ioctl did not return
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
