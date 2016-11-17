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
from os import readlink, unlink, stat, major, minor
from os.path import exists, join
from subprocess import Popen, PIPE, STDOUT
from random import random
from time import sleep

# Non-exported device-mapper constants: used for tests only.
DM_NAME_LEN = 128  # includes NULL
DM_MAX_UUID_PREFIX_LEN = 15

# Format for dmpytestN test devices.
_uuid_prefix = "DMPY-"
_uuid_format = "%s%x"

_dev_mapper = "/dev/mapper"

_udev_wait_delay = 0.1


def _new_uuid():
    rand = random() * 2 ** 64
    return _uuid_format % (_uuid_prefix, int(rand))


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


def _read_ahead_from_blockdev(dev_path):
    return int(_get_cmd_output("blockdev --getra %s" % dev_path)[1])


def _get_major_minor_from_stat(dev_path):
    st_buf = stat(dev_path)
    return (major(st_buf.st_rdev), minor(st_buf.st_rdev))


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


def _get_table_from_dmsetup(dm_name):
    return _get_cmd_output("dmsetup table %s" % dm_name)[1].strip()


def _get_driver_version_from_dmsetup():
    for line in _get_cmd_output("dmsetup version")[1].splitlines():
        if not line.startswith("Driver version"):
            continue
        return line.split(":")[1].lstrip()


def _create_stats(name, nr_areas=1, program_id="dmstats"):
    args = "--programid %s" % program_id
    if nr_areas > 1:
        args += " --areas %d" % nr_areas

    r = _get_cmd_output("dmstats create %s %s" % (args, name))
    if r[0]:
        raise OSError("Failed to create stats region")

def _remove_all_stats(name):
    # We use --allprograms --allregions here to work around dmstats versions
    # that are missing commit 5eda393:
	#
	#  commit 5eda3934885b23ce06f862a56b524ceaab3cb565
	#  Author: Bryn M. Reeves <bmr@redhat.com>
	#  Date:   Mon Oct 24 17:21:18 2016 +0100
	#
	#     dmsetup: obey --programid when deleting regions
	#
    # Without this fix attempting to delete our own regions with --programid
	# fails to remove all regions, causing assertion failures when dmpy tests
	# attempt to validate the expected number of regions.
    r = _get_cmd_output("dmstats delete --allprograms --allregions %s" % name)
    if r[0]:
        raise OSError("Failed to remove stats regions.")

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


def _create_linear_device(dev, size, uuid=_new_uuid):
    dm_name = "dmpytest0"
    sectors = size >> 9

    # if UUID is callable, call it.
    if hasattr(uuid, "__call__"):
        uuid = uuid()

    uuid_arg = ("--uuid %s" % uuid) if uuid else ""
    r = _get_cmd_output("dmsetup create %s %s "
                        "--table='0 %d ""linear %s 0'" %
                        (dm_name, uuid_arg, sectors, dev))
    if r[0]:
        raise OSError("Failed to create linear device.")

    return dm_name


def _remove_dm_device(dm_dev):
    r = _get_cmd_output("dmsetup remove %s" % dm_dev)
    if r[0]:
        raise OSError("Failed to remove dm device %s." % dm_dev)


class DmpyTests(unittest.TestCase):

    test_dev_size_bytes = 2**20
    test_dev_size_sectors = 2**11
    nodev = "quxquxquxquxqux"
    loop0 = None
    dmpytest0 = None
    program_id = "dmpytest"

    def udev_settle(self):
        _get_cmd_output("udevadm settle")

    def loop_minor(self, loop_name):
        return int(loop_name.split("loop")[1])

    def setUp(self):
        uuid = _new_uuid()
        dev_size = self.test_dev_size_bytes
        self.loop0 = _create_loopback("/var/tmp/", dev_size)
        self.dmpytest0_uuid = uuid
        self.dmpytest0 = _create_linear_device(self.loop0[0],
                                               dev_size, uuid=uuid)

    def tearDown(self):
        self.udev_settle()
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
        self.assertEqual(initial_mode, dm.STRING_MANGLING_AUTO)
        # Assert that we get each mangline mode back as expected.
        self.assertTrue(dm.set_name_mangling_mode(dm.STRING_MANGLING_NONE))
        self.assertEqual(dm.get_name_mangling_mode(), dm.STRING_MANGLING_NONE)
        self.assertTrue(dm.set_name_mangling_mode(dm.STRING_MANGLING_AUTO))
        self.assertEqual(dm.get_name_mangling_mode(), dm.STRING_MANGLING_AUTO)
        self.assertTrue(dm.set_name_mangling_mode(dm.STRING_MANGLING_HEX))
        self.assertEqual(dm.get_name_mangling_mode(), dm.STRING_MANGLING_HEX)

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
        self.assertEqual(initial_dev_dir, default_dev_dir_get)
        self.assertTrue(dm.set_dev_dir(other_dev_dir))
        self.assertEqual(dm.get_dev_dir(), other_dev_dir_get)
        self.assertTrue(dm.set_dev_dir(reset_dev_dir))
        self.assertEqual(dm.get_dev_dir(), default_dev_dir_get)

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
        self.assertEqual(initial_sysfs_dir, default_sysfs_dir_get)

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
        self.assertEqual(dm.get_uuid_prefix(), default_uuid_prefix)
        self.assertTrue(dm.set_uuid_prefix(new_uuid_prefix))
        self.assertEqual(dm.get_uuid_prefix(), new_uuid_prefix)

    def test_lib_release_releases_fd(self):
        # Assert that a call to dmpy.lib_release() closes the ioctl file
        # descriptor.
        import dmpy as dm
        dev_mapper_control = join(_dev_mapper, "control")
        control_fd_path = "/proc/self/fd/3"

        # Run a DM_DEVICE_LIST to open the ioctl fd.
        dmt = dm.DmTask(dm.DM_DEVICE_LIST)
        dmt.run()
        dmt = None
        # Control fd path should exist now.
        self.assertTrue(exists(control_fd_path))
        self.assertEqual(dev_mapper_control, readlink(control_fd_path))
        # Control fd path should be close following lib_release().
        dm.lib_release()
        self.assertFalse(exists(control_fd_path))

    def test_hold_control_dev_open(self):
        # Assert that dmpy.hold_control_dev_open() returns True, that the
        # control device is held open across a subsequent call to
        # dm.lib_release(), and that it is closed after hold_control_dev is
        # disabled and a second call to dm_lib_release() made.
        import dmpy as dm
        dev_mapper_control = join(_dev_mapper, "control")
        control_fd_path = "/proc/self/fd/3"

        # Enable holding the control fd.
        dm.hold_control_dev(1)
        # Run a DM_DEVICE_LIST to open the ioctl fd.
        dmt = dm.DmTask(dm.DM_DEVICE_LIST)
        dmt.run()
        dmt = None
        # Control fd path should exist now.
        self.assertTrue(exists(control_fd_path))
        self.assertEqual(dev_mapper_control, readlink(control_fd_path))
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
        self.assertEqual(dmpy_drv_version, dmsetup_drv_version)

    def test_dump_memory(self):
        # FIXME: test with custom logging fn?
        pass

    def test_set_get_udev_sync(self):
        # Assert that we get the expected result back after setting the
        # udev synchronization mode.
        import dmpy as dm

        # Change this if your libdevmapper was built with differnt defaults.
        initial_udev_sync = 1

        self.assertEqual(dm.udev_get_sync_support(), initial_udev_sync)
        # Try turning it off and on again.
        dm.udev_set_sync_support(0)
        self.assertEqual(dm.udev_get_sync_support(), 0)
        dm.udev_set_sync_support(1)
        self.assertEqual(dm.udev_get_sync_support(), 1)

    def test_set_get_udev_checks(self):
        # Assert that we get the expected result back after setting the
        # udev synchronization mode.
        import dmpy as dm

        # Change this if your libdevmapper was built with differnt defaults.
        initial_udev_check = 1

        self.assertEqual(dm.udev_get_checking(), initial_udev_check)
        # Try turning it off and on again.
        dm.udev_set_checking(0)
        self.assertEqual(dm.udev_get_checking(), 0)
        dm.udev_set_checking(1)
        self.assertEqual(dm.udev_get_checking(), 1)

    def test_cookie_supported(self):
        # Assert that the library returns the expected value of cookie_supported
        # depending on the library major/minor version values.
        import dmpy as dm
        (major, minor, patch) = map(int, dm.driver_version().split("."))
        if major >= 4 and minor >=15:
            self.assertTrue(dm.cookie_supported())
        else:
            self.assertFalse(dm.cookie_supported())

    def test_udev_create_cookie(self):
        # Assert that a new cookie, with non-zero value is created following a
        # call to dmpy.udev_create_cookie().
        import dmpy as dm
        cookie = dm.udev_create_cookie()
        self.assertTrue(cookie)
        self.assertTrue(cookie.value)
        self.assertTrue(cookie.udev_wait())

    def test_message_supports_precise_timestamps(self):
        # Assert that dm.message_supports_precise_timestamps() returns True.
        # FIXME: a better test would be to check the driver version from
        # dmsetup and assertTrue/assertFalse as appropriate. Today though,
        # it's unlikely that anyone running the suite is using a kernel old
        # enough to fail the test. Users of RHEL5, or older builds of RHEL6
        # and RHEL7 will fail this test.
        import dmpy as dm
        self.assertTrue(dm.message_supports_precise_timestamps())

    def test_stats_driver_supports_precise(self):
        # Assert that dm.stats_driver_supports_precise() returns True.
        # FIXME: see test_message_supports_precise_timestamps.
        import dmpy as dm
        self.assertTrue(dm.stats_driver_supports_precise())

    def test_stats_driver_supports_histogram(self):
        # Assert that dm.stats_driver_supports_histogram() returns True.
        # FIXME: see test_message_supports_precise_timestamps.
        import dmpy as dm
        self.assertTrue(dm.stats_driver_supports_histogram())

    def test_stats_all_programs(self):
        # Assert that the dmpy.STATS_ALL_PROGRAMS constant exists and has
        # the expected value.
        import dmpy as dm
        self.assertFalse(dm.STATS_ALL_PROGRAMS)
        self.assertEqual(dm.STATS_ALL_PROGRAMS, "")

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
            self.assertEqual(type(name[0]), str)  # name
            self.assertEqual(type(name[1]), int)  # major
            self.assertEqual(type(name[2]), int)  # minor

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

    def test_task_set_get_name(self):
        # Assert that `DmTask.set_name()` sets the dm name for a DM_DEVICE_INFO
        # ioctl, and returns the correct device information by checking
        # `DmTask.get_name()`.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        dmt.run()
        self.assertEqual(self.dmpytest0, dmt.get_name())

    def test_task_get_info(self):
        # Assert that a non-NULL DmInfo object is returned following a
        # successful DM_DEVICE_INFO ioctl.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        dmt.run()
        info = dmt.get_info()
        self.assertTrue(info)

    def test_task_info_fields_present(self):
        # Assert that the info.exists flag is non-zero for a valid device, and
        # that the dmpytest0 device has an active table, and is read-write.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        dmt.run()
        info = dmt.get_info()
        self.assertTrue(info.exists)
        self.assertTrue(info.live_table)
        self.assertFalse(info.read_only)

    def test_task_info_fields_nodev(self):
        # Assert that the info.exists flag is zero for a non-existent device.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.nodev)
        dmt.run()
        info = dmt.get_info()
        self.assertFalse(info.exists)

    def test_get_uuid(self):
        # Get the device UUID with a DM_DEVICE_INFO ioctl, and assert that
        # the command succeeds, and that the returned UUID string matches
        # the stored value.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        dmt.run()
        self.assertEqual(dmt.get_uuid(), self.dmpytest0_uuid)

    def test_get_deps(self):
        # Assert that a deps list is returned following a DM_DEVICE_DEPS
        # command, and that the major/minor number(s) of the dependencies
        # are as expected.
        import dmpy as dm
        # Stat the device for comparison
        (maj_stat, min_stat) = _get_major_minor_from_stat(self.loop0[0])
        dmt = dm.DmTask(dm.DM_DEVICE_DEPS)
        dmt.set_name(self.dmpytest0)
        dmt.run()
        deps = dmt.get_deps()
        # Expects: [(maj,min)]
        self.assertEqual(len(deps), 1)
        self.assertEqual(deps[0][0], maj_stat)
        self.assertEqual(deps[0][1], min_stat)

    def test_set_message_run_response(self):
        # Assert that setting a message succeeds, and that the ioctl runs
        # successfully and gives the expected response.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_TARGET_MSG)
        # Use a '@stats_create' as the message type - it will always succeed
        # on any target and system with stats support.
        dmt.set_name(self.dmpytest0)
        message = "@stats_create 0+%d /1" % self.test_dev_size_sectors
        self.assertTrue(dmt.set_message(message))
        dmt.run()
        region_id = dmt.get_message_response()
        self.assertTrue(int(region_id) >= 0)

    def test_set_newname_run_get_name(self):
        # Assert that setting a new name succeeds, and that the ioctl runs
        # successfully and returns the new name.
        import dmpy as dm
        newname = "dmpytest1"
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dmt.set_name(self.dmpytest0)
        dmt.set_newname(newname)
        dmt.run()
        self.dmpytest0 = newname  # for tearDown()
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(newname)
        dmt.run()
        self.assertEqual(dmt.get_name(), newname)

    def test_set_newuuid_with_no_uuid(self):
        # Assert that we can set a new UUID for a device that has none,
        # and that the new UUID is returned as expected.
        import dmpy as dm
        # We need a device with no UUID set.
        _remove_dm_device(self.dmpytest0)
        dmt = dm.DmTask(dm.DM_DEVICE_CREATE)
        dmpyuuidtest0 = "dmpyuuidtest0"
        self.dmpytest0 = dmpyuuidtest0  # for tearDown()
        dmt.set_name(dmpyuuidtest0)
        dmt.run()

        # Wait for udev to catch up
        self.udev_settle()

        # Generate a new UUID and apply it to the test device with a
        # DM_DEVICE_RENAME task.
        newuuid = _new_uuid()
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dmt.set_name(self.dmpytest0)
        # Assert that the new UUID is set.
        self.assertTrue(dmt.set_newuuid(newuuid))
        dmt.run()

        # Get a DM_DEVICE_INFO of the device to compare.
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        dmt.run()

        # Assert that the retrieved UUID matches.
        self.assertEqual(dmt.get_uuid(), newuuid)

    def test_newuuid_with_uuid_set_fails(self):
        # Assert that attempting to set a UUID on an active device that
        # already has one set raises an exception.
        import dmpy as dm

        # Generate a new UUID and apply it to the test device with a
        # DM_DEVICE_RENAME task.
        newuuid = _new_uuid()
        dmt = dm.DmTask(dm.DM_DEVICE_RENAME)
        dmt.set_name(self.dmpytest0)

        # Assert that the new UUID is set: this should succeed, and the
        # subsequent dmt.run() should raise an exception based on the
        # errno set by the task ioctl.
        self.assertTrue(dmt.set_newuuid(newuuid))

        with self.assertRaises(OSError) as cm:
            dmt.run()

        self.assertEqual(dmt.get_errno(), 22)  # EINVAL

    def test_task_get_driver_version(self):
        # Assert that we can obtain the driver version from a task, and
        # that the result matches that obtained from dmsetup.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        dmt.run()
        _dmsetup_driver_version = _get_driver_version_from_dmsetup()
        _driver_version = dmt.get_driver_version()
        self.assertTrue(_driver_version)
        self.assertEqual(_dmsetup_driver_version, _driver_version)

    def test_task_set_major_and_set_minor(self):
        # Send a DM_DEVICE_INFO task by major and minor number, and assert
        # that the expected device name is returned.
        import dmpy as dm
        dev_path = join(_dev_mapper, self.dmpytest0)
        (major, minor) = _get_major_minor_from_stat(dev_path)
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_major(major)
        dmt.set_minor(minor)
        dmt.run()
        self.assertEqual(dmt.get_name(), self.dmpytest0)

    def test_task_set_major_minor(self):
        # Send a DM_DEVICE_INFO task by major and minor number, and assert
        # that the expected device name is returned.
        import dmpy as dm
        dev_path = join(_dev_mapper, self.dmpytest0)
        (major, minor) = _get_major_minor_from_stat(dev_path)
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_major_minor(major, minor)
        dmt.run()
        self.assertEqual(dmt.get_name(), self.dmpytest0)

    def test_task_set_uid(self):
        # Setting UIDs should be handled by udev rules now.
        pass

    def test_task_set_gid(self):
        # Setting GIDs should be handled by udev rules now.
        pass

    def test_task_set_mode(self):
        # Setting device node modes should be handled by udev rules now.
        pass

    def test_task_get_errno(self):
        # Run a DM_DEVICE_INFO ioctl for a non-existent device, and assert
        # that the errno returned by DmTask.get_errno() is 6 (ENXIO).
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.nodev)
        dmt.run()
        self.assertEqual(dmt.get_errno(), 6)

    def test_task_set_sector(self):
        # Assert that setting a message succeeds, and that the ioctl runs
        # successfully and gives the expected response.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_TARGET_MSG)
        dmt.set_name(self.dmpytest0)
        dmt.set_message("@stats_list")
        self.assertTrue(dmt.set_sector(0))
        dmt.run()
        response = dmt.get_message_response()
        self.assertEqual(response, "")

    def test_task_no_flush(self):
        # Assert that setting noflush on a DM_DEVICE_SUSPEND task succeeds.
        # FIXME: no testing of the flag's behaviour is done.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_SUSPEND)
        dmt.set_name(self.dmpytest0)
        self.assertTrue(dmt.no_flush())
        dmt.run()

    def test_task_no_open_count(self):
        # Assert that setting no_open_count on a DM_DEVICE_INFO task succeeds,
        # and that the resulting task open count is zero.
        import dmpy as dm
        dmt = dm.DmTask(dm.DM_DEVICE_INFO)
        dmt.set_name(self.dmpytest0)
        self.assertTrue(dmt.no_open_count())
        dmt.run()
        info = dmt.get_info()
        # NOTE: with kernel-4.7 and device-mapper 1.02.122 the open count is
        # returned regardless of the setting of no_open_count: the following
        # assertion is true simply because the test device is not open. If
        # the block is enclosed in a 'with' clause that opens the device, it
        # will fail.
        #
        # The code that implemented "no_open_count" was removed from the
        # kernel in 2006:
        #
        #  commit 5c6bd75d06db512515a3781aa97e42df2faf0815
        #  Author: Alasdair G Kergon <agk@redhat.com>
        #  Date:   Mon Jun 26 00:27:34 2006 -0700
        #
        #  [PATCH] dm: prevent removal if open
        #
        # And the DM_SKIP_BDGET_FLAG that implements it was marked as
        # ignored:
        #
        # /*
        #  * This flag is now ignored.
        #  */
        # #define DM_SKIP_BDGET_FLAG      (1 << 9) /* In */
        #
        # self.assertFalse(info.open_count)

    def test_task_set_geometry(self):
        # Assert that setting the geometry strings succeeds,
        import dmpy as dm
        geometry = ("62260", "255", "63", "64")
        dmt = dm.DmTask(dm.DM_DEVICE_SET_GEOMETRY)
        self.assertTrue(dmt.set_geometry(*geometry))
        dmt.set_name(self.dmpytest0)
        # FIXME: kernel bug?
        # Trying to run this task, with kernel 4.7 and device-mapper 1.02.122
        # fails with EINVAL, and an error in dmesg:
        #
        #   ioctl: Unable to interpret geometry settings.
        #
        # Indicating the wrong number of string arguments received by
        # drivers/md/dm-ioctl.c:dev_set_geometry().
        #dmt.run()

    def test_task_create_single_linear_target_udev(self):
        # Attempt to create a simple device with a single, linear target,
        # and assert that the device node exists, and that the device
        # table (as reported by dmsetup) matches the expected table.
        import dmpy as dm
        # Use a new device name - we will create and destroy it during
        # the test.
        dmpytest1 = "dmpytest1"
        loop_minor = self.loop_minor(self.loop0[0])
        expected_table = "0 2048 linear 7:%d 0" % loop_minor
        dmt = dm.DmTask(dm.DM_DEVICE_CREATE)
        dmt.set_name(dmpytest1)
        cookie = dm.DmCookie()
        self.assertTrue(dmt.set_cookie(cookie))

        self.assertTrue(dmt.add_target(0, self.test_dev_size_sectors,
                                       "linear", "%s 0" % self.loop0[0]))
        dmt.set_uuid(_new_uuid())
        dmt.run()

        # Wait for udev to complete
        self.assertTrue(cookie.udev_wait())

        # Assert that the device node exists
        self.assertTrue(exists(join(_dev_mapper, dmpytest1)))

        table = _get_table_from_dmsetup(dmpytest1)
        self.assertEqual(table, expected_table)

        # Remove the device and its node
        dmt = dm.DmTask(dm.DM_DEVICE_REMOVE)
        dmt.set_name(dmpytest1)
        cookie = dm.DmCookie()
        self.assertTrue(dmt.set_cookie(cookie))
        dmt.run()

        # Wait for udev to complete
        self.assertTrue(cookie.udev_wait())

        # Assert that the device node no longer exists
        self.assertFalse(exists(join(_dev_mapper, dmpytest1)))

    def test_task_create_single_linear_target_no_udev(self):
        # Attempt to create a simple device with a single, linear target,
        # and assert that the device node exists, and that the device
        # table (as reported by dmsetup) matches the expected table.
        import dmpy as dm
        # Use a new device name - we will create and destroy it during
        # the test.
        dmpytest1 = "dmpytest1"
        loop_minor = self.loop_minor(self.loop0[0])
        expected_table = "0 2048 linear 7:%d 0" % loop_minor
        dm.udev_set_sync_support(0)
        dmt = dm.DmTask(dm.DM_DEVICE_CREATE)
        dmt.set_name(dmpytest1)
        self.assertTrue(dmt.add_target(0, self.test_dev_size_sectors,
                                       "linear", "%s 0" % self.loop0[0]))
        dmt.set_uuid(_new_uuid())
        dmt.run()

        # update device nodes
        dm.update_nodes()

        # Assert that the device node exists
        self.assertTrue(exists(join(_dev_mapper, dmpytest1)))

        table = _get_table_from_dmsetup(dmpytest1)
        self.assertEqual(table, expected_table)

        # Remove the device and its node
        dmt = dm.DmTask(dm.DM_DEVICE_REMOVE)
        dmt.set_name(dmpytest1)
        dmt.run()

        # update device nodes
        dm.update_nodes()

        # Assert that the device node no longer exists
        self.assertFalse(exists(join(_dev_mapper, dmpytest1)))
        dm.udev_set_sync_support(1)

    #
    # Cookie tests
    #

    def test_new_cookie_not_ready(self):
        # Create a new cookie, assert that it is not ready, and then
        # destroy it.
        import dmpy as dm
        cookie = dm.udev_create_cookie()
        self.assertFalse(cookie.ready)
        cookie.udev_wait()

    def test_cookie_ready_after_wait(self):
        # Create a new cookie, wait on it, and assert that it becomes
        # ready following the call to cookie.udev_wait().
        import dmpy as dm
        cookie = dm.udev_create_cookie()
        self.assertFalse(cookie.ready)
        self.assertTrue(cookie.udev_wait())
        self.assertTrue(cookie.ready)

    def test_cookie_multiple_wait(self):
        # Create a new cookie, wait on it, and assert that a further attempt
        # to call udev_wait() raises a ValueError exception.
        import dmpy as dm
        cookie = dm.udev_create_cookie()
        self.assertFalse(cookie.ready)
        self.assertTrue(cookie.udev_wait())
        self.assertTrue(cookie.ready)
        with self.assertRaises(ValueError) as cm:
            cookie.udev_wait()

    def test_cookie_wait_immediate(self):
        # Create a new cookie, wait on it, and assert that it becomes ready.
        import dmpy as dm
        _remove_dm_device(self.dmpytest0)
        dmt = dm.DmTask(dm.DM_DEVICE_CREATE)
        dmt.set_name(self.dmpytest0)
        cookie = dm.udev_create_cookie()
        self.assertFalse(cookie.ready)
        dmt.set_cookie(cookie)
        dmt.add_target(0, self.test_dev_size_sectors,
                       "linear", "%s 0" % self.loop0[0])
        dmt.run()

        cookie.udev_wait(immediate=True)
        while not cookie.ready:
            cookie.udev_wait(immediate=True)
            sleep(_udev_wait_delay)

        self.assertTrue(cookie.ready)

    #
    # DmStats tests
    #

    def test_stats_create_program_id(self):
        # Assert that creating a DmStats handle with program_id=None
        # returns a valid object.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        self.assertTrue(dms.__init__)
        self.assertEqual(type(dms), dm.DmStats)

    def test_stats_create_all_programs(self):
        # Assert that creating a DmStats handle with dm.STATS_ALL_PROGRAMS
        # returns a valid object.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertTrue(dms.__init__)
        self.assertEqual(type(dms), dm.DmStats)

    def test_stats_create_no_program_id(self):
        # Assert that creating a DmStats handle with program_id=None
        # returns a valid object.
        import dmpy as dm
        dms = dm.DmStats(None)
        self.assertTrue(dms.__init__)
        self.assertEqual(type(dms), dm.DmStats)

    def test_stats_create_bind_name(self):
        # Assert that creating a DmStats handle and binding it to a name via
        # the name= keword argument returns a valid object.
        import dmpy as dm
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.__init__)
        self.assertEqual(type(dms), dm.DmStats)

    def test_stats_create_no_program_id_bind_name(self):
        # Assert that creating a DmStats handle with program_id=None
        # and a name= keyword argument returns a valid object.
        import dmpy as dm
        dms = dm.DmStats(None, name=self.dmpytest0)
        self.assertTrue(dms.__init__)
        self.assertEqual(type(dms), dm.DmStats)

    def test_stats_create_multiple_bind_raises(self):
        # Assert that attempting to pass multiple device binding kwargs
        # raises a TypeError exception.
        import dmpy as dm
        with self.assertRaises(TypeError) as cm:
            dm.DmStats(self.program_id, name="foo", uuid="qux")

    def test_stats_bind_name_none(self):
        # Assert that attempting to bind a NULL name raises TypeError.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        with self.assertRaises(TypeError) as cm:
            dms.bind_name(None)

    def test_stats_bind_name_empty(self):
        # Assert that attempting to bind an empty name raises ValueError.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        with self.assertRaises(ValueError) as cm:
            dms.bind_name("")

    def test_stats_bind_name_valid(self):
        # Assert that attempting to bind a valid name succeeds.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        self.assertTrue(dms.bind_name(self.dmpytest0))

    def test_stats_bind_uuid_none(self):
        # Assert that attempting to bind a NULL uuid raises TypeError.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        with self.assertRaises(TypeError) as cm:
            dms.bind_uuid(None)

    def test_stats_bind_uuid_empty(self):
        # Assert that attempting to bind an empty uuid raises ValueError.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        with self.assertRaises(ValueError) as cm:
            dms.bind_uuid("")

    def test_stats_bind_uuid_valid(self):
        # Assert that attempting to bind a valid uuid succeeds.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        self.assertTrue(dms.bind_uuid(_new_uuid()))

    def test_stats_bind_devno_valid(self):
        # Assert that attempting to bind a valid devno succeeds.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        # FIXME: generate a valid DM major/minor pair.
        self.assertTrue(dms.bind_devno(253, 0))

    def test_stats_new_has_no_regions(self):
        # Assert that a newly created / bound stats handle returns zero
        # regions.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertTrue(dms.__init__)
        self.assertFalse(dms.nr_regions())

    def test_stats_new_has_no_groups(self):
        # Assert that a newly created / bound stats handle returns zero
        # groups.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertTrue(dms.__init__)
        self.assertFalse(dms.nr_groups())

    def test_stats_new_has_no_areas(self):
        # Assert that a newly created / bound stats handle returns zero
        # areas.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertTrue(dms.__init__)
        self.assertFalse(dms.nr_areas())

    def test_stats_not_present_region_is_not_present(self):
        # Assert that a non-existent region_id returns False when passed
        # to DmStats.region_present().
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertFalse(dms.region_present(-1))

    def test_stats_new_handle_region_has_no_areas(self):
        # Assert that a newly created / bound stats handle returns zero
        # areas.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertTrue(dms.__init__)
        self.assertFalse(dms.region_nr_areas(0))

    def test_stats_new_handle_no_group_present(self):
        # Assert that groups are not present in a newly created DmStats object.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertFalse(dms.group_present(0))
        self.assertFalse(dms.group_present(1))
        self.assertFalse(dms.group_present(-1))

    def test_stats_set_get_sampling_interval(self):
        # Assert that setting a sampling interval is raises no error, and that
        # the set value is returned by a subsequent get.
        import dmpy as dm
        dms = dm.DmStats(dm.STATS_ALL_PROGRAMS)
        self.assertTrue(dms.set_sampling_interval(0.5))
        self.assertEqual(dms.get_sampling_interval(), 0.5)

    def test_stats_set_none_program_id_no_allow_empty_raises(self):
        # Assert that attempting to set None as the program_id fails if the
        # allow_empty flag is not given.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        with self.assertRaises(ValueError) as cm:
            dms.set_program_id(None)
        with self.assertRaises(ValueError) as cm:
            dms.set_program_id("")

    def test_stats_set_none_program_id_with_allow_empty(self):
        # Assert that attempting to set None as the program_id fails if the
        # allow_empty flag is not given.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        dms.set_program_id(None, allow_empty=True)
        dms.set_program_id("", allow_empty=True)

    def test_stats_set_program_id(self):
        # Assert that setting a valid program_id succeeds.
        import dmpy as dm
        dms = dm.DmStats(self.program_id)
        self.assertTrue(dms.set_program_id("qux"))

    def test_stats_list(self):
        # Assert that listing an empty device yields an empty
        # DmStats object, and that the correct number of regions is
        # returned when listing a device with regions present.
        import dmpy as dm
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertFalse(dms.list())
        self.assertEqual(len(dms), 0)
        _create_stats(self.dmpytest0, nr_areas=1, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.list())
        self.assertEqual(len(dms), 1)
        _remove_all_stats(self.dmpytest0)
        _create_stats(self.dmpytest0, nr_areas=4, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.list())
        self.assertEqual(len(dms), 1)

    def test_stats_populate_empty(self):
        # Assert that populating an empty device yields an empty
        # DmStats object, and that the correct number of regions is
        # returned when populating a device with regions present.
        import dmpy as dm
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        with self.assertRaises(OSError) as cm:
            dms.populate()
        self.assertEqual(len(dms), 0)

    def test_stats_populate_unlisted_raises(self):
        import dmpy as dm
        _create_stats(self.dmpytest0, nr_areas=1, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        # Attempting to populate a single region in an empty dm_stats handle
        # is an error (since the region tables have not been dimensioned):
        # a single-region populate() without a prior list() should raise an
        # OSError exception.
        with self.assertRaises(OSError) as cm:
            dms.populate(region_id=0)
        self.assertEqual(len(dms), 0)

    def test_stats_populate_bogus_raises(self):
        import dmpy as dm
        _create_stats(self.dmpytest0, nr_areas=1, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.list())
        with self.assertRaises(OSError) as cm:
            dms.populate(region_id=1)  # dms has only region_id=0
        self.assertEqual(len(dms), 0)

    def test_stats_populate_one_region(self):
        import dmpy as dm
        _create_stats(self.dmpytest0, nr_areas=1, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.list())
        self.assertTrue(dms.populate(region_id=0))
        self.assertEqual(len(dms), 1)

    def test_stats_populate_all_regions(self):
        import dmpy as dm
        _create_stats(self.dmpytest0, nr_areas=1, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.populate())
        self.assertEqual(len(dms), 1)

    #
    # DmStatsRegion tests.
    #

    def test_region_nr_areas(self):
        # Assert that listing an empty device yields an empty
        # DmStats object, and that the correct number of regions is
        # returned when listing a device with regions present.
        import dmpy as dm
        _create_stats(self.dmpytest0, nr_areas=1, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.list())
        self.assertEqual(len(dms), 1)
        self.assertEqual(dms[0].nr_areas(), 1)
        _remove_all_stats(self.dmpytest0)
        _create_stats(self.dmpytest0, nr_areas=4, program_id=self.program_id)
        dms = dm.DmStats(self.program_id, name=self.dmpytest0)
        self.assertTrue(dms.list())
        self.assertEqual(len(dms), 1)
        self.assertEqual(dms[0].nr_areas(), 4)

# vim: set et ts=4 sw=4 :
