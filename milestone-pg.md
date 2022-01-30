# Heapstore Page (pg)

In this milestone you will be building the first step of a storage manager that uses heapfiles to store values/data. In CrustyDB a storage manager (**SM**) is responsible for persisting all data. A SM in Crusty is agnostic to what is being stored, as it takes a request to store a `value` as bytes (a `Vec<u8>`) in a `container`. The value is stored and the SM returns a `ValueId` that indicates how it can retrieve the value later. It is the responsibility of an other component in the system to interpret the bytes. For example, CrustyDB will create a container for each table/relation stored, and each record will get stored as a `value`.  The same database could also store an index as a container, and store each index page as a `value`.

In this milestone you will focus on one piece of functionality of the heapstore, the **page**.
This page is a fixed sized data structure to hold variable sized records using a slotted storage, which each inserted record gets a slot that points to a contigious space in the page that holds the bytes for a value. A value will not be split across pages.

This milestone will have more guidance than later milestones, so for much of this milestone you will be completing required functions. The milestone includes a series of unit tests and integration tests for testing functionality. These tests are not exhaustive and you may want to write (and possibly contribute) additional tests. This module has a moderate amount of comments. Not all packages in CrustyDB will have the same level of comments, as this is designed to be the first milestone in CrustyDB. Working on a moderate sized code base with not a full set of comments or documentation is something you will encounter in your career.

## Scoring and Requirements

70% of your score on this milestone is based on correctness that is demonstrated by passing all of the provided unit and integration tests in the HS package. This means when locally running `cargo test -p heapstore hs_page` all tests pass. 10% of the score is reserved for hidden tests that we run to check correctness. 10% of your score is based on code quality (following good coding conventions, comments, well organized functions, etc). 10% is based on your write up (my-pg.txt). The write up should contain:
 -  A brief describe of your solution, in particular what design decisions you took and why. This is only needed for part of your solutions that had some significant work (e.g. just returning a counter or a pass through function has no design decision).
- How long you roughly spent on the milestone, and what would have liked/disliked on the milestone.
- If you know some part of the milestone is incomplete, write up what parts are not working, how close you think you are, and what part(s) you got stuck on.

### Logging / Logging Tests

CrustyDB uses the [env_logger](https://docs.rs/env_logger/0.8.2/env_logger/) crate for logging. Per the docs on the log crate:
```
The basic use of the log crate is through the five logging macros: error!, warn!, info!, debug! and trace! where error! represents the highest-priority log messages and trace! the lowest. 
The log messages are filtered by configuring the log level to exclude messages with a lower priority. Each of these macros accept format strings similarly to println!.
```

The logging level is set by an environmental variable, `RUST_LOG`. The easiest way to set the level is when running a cargo command you set the logging level in the same command. EG : `RUST_LOG=debug cargo run --bin server`. However, when running unit tests the logging/output is suppressed and the logger is not initialized. So if you want to use logging for a test you must:
 - Make sure the test in question calls `init()` which is defined in `common::testutils` that initializes the logger. It can safely be called multiple times.
 - Tell cargo to not capture the output. For example, setting the level to DEBUG: `RUST_LOG=debug cargo test -- --nocapture [opt_test_name]`  **note the -- before --nocapture**


### Page
The heap file is made up of a sequence of fixed sized pages (`PAGE_SIZE` in `common::lib.rs`). Note that each page may take up slightly more memory when loaded, but it must be able to be serialized (packed) into the fixed page size.  A page is broken into a header, which holds metadata about the page and values stored, and the body, which is where the bytes for values are stored.  This means that when in memory a page may use more than ` data: [u8; PAGE_SIZE]` (member of struct Page).


Note that values can differ in size,  but CrustyDB can reject any value that is larger than `PAGE_SIZE`. When a value is stored in a page it is associated with a `slot_id` that should not change, unless the value is updated. Note that the location of the bytes in a page for a value *can* change, but again the slot_id should not. When storing values in a page, the page should insert the value in the 'first' available space in the page. We quote first as it depends on your implementation what first actually means. If a value is deleted, than that space should be reused by a later insert. A page should also provide an iterator to return all of the valid values stored in the page.

### ValueId
Every stored value is associated with a `ValueId`. This is defined in `common::ids`. Each ValueId must specify a ContainerId and then a set of optional Id types. For this milestone, we will use PageId and SlotId for each ValueId. The (data) types used for these Ids are also defined in `common::ids`. 

```
pub type ContainerId = u16;
pub type AtomicContainerId = AtomicU16;
pub type SegmentId = u8;
pub type PageId = u16;
pub type SlotId = u16;
```
when casting to and from another type (usize) to these Id types, you should use the type (SlotId) as they could change with time.  The intention is a that a ValueId <= 64 bits. This means that we know a page cannot have more than SlotId slots (`2^16`).

## Suggested Steps
This is a rough order of steps we suggest you take to complete the hs milestone. Note this is not exhaustive of all required tests for the milestone.

### Page
The heap page is the basic building block of this milestone so start with this file/struct. Start by reading through the functions and comments to understand what functions are required. Think about what data structures/meta data you will need to allow for storing variable sized values. You may end up adding new helper/utility functions.

#### Add Value / Get Value
The natural starting point is `new`, `add_value`, and `get_value`.
New should create your structure and store some basic data in the header. With `new` working you have the basics to test the `hs_page_create` unit test: `cargo test -p heapstore hs_page_create`
This test requires that you add two utility functions.  `get_header_size` for getting the current header size when serialized (which will be useful for figuring out how much free space you really have) and `get_largest_free_contiguous_space` to determine the largest block of data free in the page.

With new working, move onto add_value. This should enable `hs_page_simple_insert` to pass (`cargo test -p heapstore hs_page_simple_insert`). This test adds some tuples (as bytes) to the page and then checks that (1) the slot ids are assigned in order and (2) that the largest free space and header size are aligned.

After, implement get_value and verify that `hs_page_get_value` passes.
At this point tests `hs_page_header_size_small`, `hs_page_header_size_full` and `hs_page_no_space` should also work.

Note there is no fill factor when considering wether to accept an add value. If there is enough free space on the page, it must accept the add.

#### Delete
Next implement the function `delete_value` which should free up the bytes previously used by the slot_id and also make the slot_id available for the next insert/add. Start with the test `hs_page_simple_delete` which only verifies that deleted values are gone. Once this is working you will want to make sure that you are reusing the space/slots. I would suggest writing a utility function that lets you find the first free space in a page and test this function with `hs_page_get_first_free_space` which needs to be written. Here you might want to explore inserting bytes vectors of different sizes and see if you can replace/reuse the space as effectively as possible (e.g., two `_b2`'s should replace one deleted `_b1`).  You should have `hs_page_delete_insert` working also at this point.

#### Serialize/get_bytes and deserialize/from_bytes
Next write the methods to create the byte vector from a page (`get_bytes`) and the method to create a page from a reference/borrow to array of bytes. You cannot rely on any serde library for this and must ensure that the data fits into `PAGE_SIZE`. Some hints are available in the function comments. With these functions working `hs_page_size` and `hs_page_simple_byte_serialize` should pass.

### Page Iterator
The last component of the page is writing an iterator to 'walk' through all valid values stored in a page. This is a consuming iterator which will move/take ownership of the page. You will want to fill in the struct `PageIter` to hold the metadata for the iterator, the `next` function in the `impl Iterator for PageIter`, and `into_iter` in `impl IntoIterator for Page` that creates the iterator from a page. With these functions `hs_page_iter` should pass.

After completing the iterator all required functionality in the page should be complete and you can run all the tests in the file by running `cargo test -p heapstore hs_page_` Ensure that you did not break any tests! Congrats! 


## Space Use/Reclamation Example

Deleted space should be used again by the page, but there is no requirement as to when. In other words you should never decline an add_value when the free space does exist on the page.

Imagine we have a page with the following "free spaces" (with a stored value possibly requiring more than one "space") Repeating letters are store values and - indicates a free space.

We have a value AA, a value B, and a value CC, and 3 free spaces (-). SlotIds of AA,B, and CC are 0,1,2.

```
AABCC---
```

We delete B

```
AA-CC---
```

When inserting D, we could use a - between A & C [`AADCC---`] or a - after CCC [`AA-CCD--`]. We go with the later. The slotId of D should be 1 either way (re-using B's SlotId).

```
AA-CCD--
```

Inserting EE has only one viable spot/space.  The slotId of EE should be 3.

```
AA-CCDEE
```

Inserting FF should reject (return None) as it's too large. No slotId.

Inserting G must be accepted as there is room. We cannot leave a 'zombie' space to never be reclaimed. The slotId of G should be 4.

```
AAGCCDEE
```