# Hacking DmPy

1. [Overview](#s1)
1. [Mapping the dmstats API to Python](#s2)
1. [DmPy reference counting and caching](#s3)
  1.1 [DmStats sequence numbers](#s3.1)
1. [DmTask API state flags](#s4)

## 1. Overview <a name="s1"/></a>
This file explains the object structure, reference counting and caching
scheme used by the DmPy Python bindings for device-mapper, and is useful
for anyone wishing to extend the bindings or modify or extend the Python
container classes provided by the library using the C interface (users
wishing to use or extend the classes in Python itself do not need to
worry about these details: the provided classes conform to normal Python
reference counting behaviour).


## 2. Mapping the dmstats API to Python <a name="s2"/></a>
The dmstats C API returns opaque pointers to complex objects
representing the state of a dmstats handle, and any regions, areas,
groups, histograms or other objects associated with the handle.

Internally they form a tree-like structure rooted in an instance of type
`struct dm_stats`:

```
  dm_stats
      regions[nr_regions]->dm_stats_region
                            counters[nr_areas]->dm_stats_counters
                            histogram->dm_histogram
                                        bins[nr_bins]->dm_histogram_bin
      groups[nr_groups]->dm_stats_group
                          histogram->dm_histogram
                                      bins[nr_bins]->dm_histogram_bin
```

The C API only exports this structure via C function calls acting on the
`dm_stats` and `dm_histogram` handles (effectively methods of the
`dm_stats` and `dm_histogram` types: region, area and bin indices are
parameters of these methods).

It is desirable to expose this structure in the Python API as a set of
native Python classes that implement common Python idioms for object
traversal. For example, indexable objects (the containers of groups,
regions, areas, and histogram bins) should implement sequence methods
(`PyTypeObject.tp_as_sequence`) to allow the user to access objects with
the indexing operator (`object[index]`).

This allows users of the API to write to the DmStats API using familiar
Python conventions such as:

```python
        # Get a reference to region 0 and access attributes
        region = dms[0]
        print(region.nr_areas)

        # Index into region 1, area 0 through a DmStats object
        print(dms[1][0].READS_COUNT)
        print(dms[1][0].READS_MERGED_COUNT)
        print(dms[1][0].READ_SECTORS_COUNT)
        print(dms[1][0].READ_NSECS)

```

For each device-mapper type, a corresponding Python class is introduced:
these serve as shims or wrappers around the actual data contained in the
underlying `dm_stats` handle. The wrapper intentionally stores as little
state as possible: all data is returned directly from the corresponding
C type object.


## 3. DmPy reference counting and caching <a name="s3"/></a>
Since Python is a garbage collected language objects in the Python type
system are reference counted: objects with a reference count of zero are
subject to possible garbage collection and disposal at any time. To
prevent access to memory that has already been deallocated, objects that
depend on accessing other objects for their state must maintain a
reference to those objects to prevent their deallocation.

Like other garbage collected languages Python supports the notion of
both strong and weak references; a strong reference prevents garbage
collection, where a weak reference does not. A weak reference can return
a reference to the referent object if it still exists, or an error
indicating that the object has already been destroyed. Strong and weak
references can be used to eliminate garbage collection reference cycles
which would otherwise delay the deallocation of some resources
indefinitely.

Mixtures of strong and weak references are frequently used in data
structures such as lists, trees, graphs and complex aggregate objects
where the possibility of cyclical references exists.

Many conventional tree-like structures are able to implement a scheme in
which objects hold strong references 'down' the tree, i.e. from the root
node towards leaf nodes, and child nodes hold weak references 'up' the
tree (toward their parents). This gives the desired behaviour in the
case that the root node "owns" the nodes at lower levels, for example an
aggregate structure where operations on the root object will need to
access nodes holding resources at lower levels of the tree. The classic
example is the HTML DOM: a page is rendered beginning at the document
root, and proceeding through the various resources represented by
lower-level nodes in the hierarchy. Child nodes cannot be deallocated
until the parent releases its strong reference and children are unable
to keep parent nodes alive and in-memory since they posess only a weak
reference.

This is the reverse of the situation in DmPy for DmStats compound
objects: in this instance, it is the root `dm_stats` object that holds
all the resources used by nodes further down in the structure: Python
objects such as DmStatsRegion and DmStatsArea must obtain data from the
original `dm_stats` handle that is contained in the DmStats object from
which they were instantiated (their parent or containing DmStats
object) as they are simple shims with no state of their own.

In this situation it is insufficient for the child nodes to hold a weak
reference: this allows a situation where the reference count on the
containing DmStats object reaches zero while children are still
oustanding with non-zero reference counts:

```python
  > dms = dm.DmStats() # dms has refcnt=1
  > dms.populate()     # dms has refcnt=1
  > region = dms[0]    # region has refcnt=1
  > dms = None         # dms has refcnt=0
  > region.nr_areas    # dms has been destroyed
  (boom)

```

To address this DmPy uses the reverse of the DOM scheme described
earlier: strong references extend from children to the container, and
containers maintain weak references to their children.

These weak references are used to avoid excessive object cycling in the
case that users repeatedly look up the same object: if the weak
reference is still alive the existing object is returned.  A new object
is only created if no weak reference is found in the cache, or if the
reference is found to be stale.

With this approach the above reference counting problem becomes
impossible: even though the user has dropped their reference to 'dms',
the 'region' instance of DmStatsRegion is itself holding a strong
reference to this object.

Each object in the DmStats hierarchy that behaves as a container
implements the same caching scheme: new code implementing a container
type should adopt a similar approach when data from the base `dm_stats`
handle or another native dmstats type must be exported.

### 3.1 DmStats sequence numbers <a name="s3.1"/></a>
Although the reference count maintained by child objects prevents the
deallocation of a `DmStats` object, the state is mutable and may be
altered in ways that invalidate the state of child objects that
reference it. Attempting to access the `DmStats` object with the
parameters stored in the child may cause illegal memory references if
those regions or areas no longer exist.

To prevent child objects attempting to access data when this occurs each
`DmStats` object stores a sequence number that is incremented every time
an operation occurs that would invalidate child references (list,
delete, populate etc.). Each child takes a copy of this sequence number
when it is initialised and compares it with the sequence number of the
parent on each lookup: if the numbers do not match a LookupError is
raised.

## 4. DmTask API state flags <a name="s4"/></a>
The `DmTask` Python class represents the struct `dm_task` C type. This is
a complex type representing all possible device-mapper `ioctl` operations
and their states: the type contains fields for both input and output and
expects users of the API to understand numerous rules regarding their
usage (for example, that data returned by an `ioctl` will only be
available once that `ioctl` has been issued, and the particular items of
data that will be valid following a spefific `ioctl` command).

As with most C APIs failing to adhere to the API rules will result in
undefined behaviour and may cause the Python interpreter to generate
segmentation faults or other memory access errors.

To allow Python users to safely access this interface the bindings
maintain a simple state field consisting of flags for `ioctl` states.
Flags are divided into state and data: state flags indicate that
something occurred (an `ioctl` was issued, or an error was returned) and
data flags indicate what data fields in the corresponding `dm_task` are
now valid:

```C
    State flags
    #define DMT_DID_IOCTL           0x00000001
    #define DMT_DID_ERROR           0x00000002

    Data flags
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

```

A table is maintained mapping each possible `ioctl` command to the set of
data flags that should be set on return from a successful `ioctl`.

State and data flags are set by the `DmTask.run()` method; attributes and
other methods that must access the data guarded by these flags should
inspect the flags and raise `TypeError` if the request is invalid for the
current task type or state. A helper function,
`_DmTask_check_data_flags()` is provided that will check that an ioctl has
been run, and that the given flags are now set. On error `TypeError` is
raised and the exception message is set to a string describing the task
type and what data is missing.

