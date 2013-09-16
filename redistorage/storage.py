from zope.interface import implements

from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.interfaces import IStorage, IMVCCStorage

from .utils import connect


class RedisStorage(ConflictResolvingStorage):

    implements(IStorage, IMVCCStorage)

    _redis = None

    def __init__(self, uri=None, pool=None, max_connections=None):
        if pool is not None:
            self._redis = pool
        elif uri is not None:
            self._redis = connect(uri, max_connections)
        else:
            raise ValueError("Either uri or pool must be specified.")

    def close():
        """Close the storage.

        Finalize the storage, releasing any external resources.  The
        storage should not be used after this method is called.
        """

    def getName():
        """The name of the storage

        The format and interpretation of this name is storage
        dependent. It could be a file name, a database name, etc..

        This is used soley for informational purposes.
        """

    def getSize():
        """An approximate size of the database, in bytes.

        This is used soley for informational purposes.
        """

    def history(oid, size=1):
        """Return a sequence of history information dictionaries.

        Up to size objects (including no objects) may be returned.

        The information provides a log of the changes made to the
        object. Data are reported in reverse chronological order.

        Each dictionary has the following keys:

        time
            UTC seconds since the epoch (as in time.time) that the
            object revision was committed.

        tid
            The transaction identifier of the transaction that
            committed the version.

        serial
            An alias for tid, which expected by older clients.

        user_name
            The user identifier, if any (or an empty string) of the
            user on whos behalf the revision was committed.

        description
            The transaction description for the transaction that
            committed the revision.

        size
            The size of the revision data record.

        If the transaction had extension items, then these items are
        also included if they don't conflict with the keys above.

        """

    def isReadOnly():
        """Test whether a storage allows committing new transactions

        For a given storage instance, this method always returns the
        same value.  Read-only-ness is a static property of a storage.
        """

        # XXX Note that this method doesn't really buy us much,
        # especially since we have to account for the fact that a
        # ostensibly non-read-only storage may be read-only
        # transiently.  It would be better to just have read-only errors.

    def lastTransaction():
        """Return the id of the last committed transaction.

        If no transactions have been committed, return a string of 8
        null (0) characters.
        """

    def __len__():
        """The approximate number of objects in the storage

        This is used soley for informational purposes.
        """

    def load(oid, version):
        """Load data for an object id

        The version argumement should always be an empty string. It
        exists soley for backward compatibility with older storage
        implementations.

        A data record and serial are returned.  The serial is a
        transaction identifier of the transaction that wrote the data
        record.

        A POSKeyError is raised if there is no record for the object id.
        """

    def loadBefore(oid, tid):
        """Load the object data written before a transaction id

        If there isn't data before the object before the given
        transaction, then None is returned, otherwise three values are
        returned:

        - The data record

        - The transaction id of the data record

        - The transaction id of the following revision, if any, or None.

        If the object id isn't in the storage, then POSKeyError is raised.
        """

    def loadSerial(oid, serial):
        """Load the object record for the give transaction id

        If a matching data record can be found, it is returned,
        otherwise, POSKeyError is raised.
        """

#     The following two methods are effectively part of the interface,
#     as they are generally needed when one storage wraps
#     another. This deserves some thought, at probably debate, before
#     adding them.
#
#     def _lock_acquire():
#         """Acquire the storage lock
#         """

#     def _lock_release():
#         """Release the storage lock
#         """

    def new_oid():
        """Allocate a new object id.

        The object id returned is reserved at least as long as the
        storage is opened.

        The return value is a string.
        """

    def pack(pack_time, referencesf):
        """Pack the storage

        It is up to the storage to interpret this call, however, the
        general idea is that the storage free space by:

        - discarding object revisions that were old and not current as of the
          given pack time.

        - garbage collecting objects that aren't reachable from the
          root object via revisions remaining after discarding
          revisions that were not current as of the pack time.

        The pack time is given as a UTC time in seconds since the
        epoch.

        The second argument is a function that should be used to
        extract object references from database records.  This is
        needed to determine which objects are referenced from object
        revisions.
        """

    def registerDB(wrapper):
        """Register a storage wrapper IStorageWrapper.

        The passed object is a wrapper object that provides an upcall
        interface to support composition.

        Note that, for historical reasons, an implementation may
        require a second argument, however, if required, the None will
        be passed as the second argument.

        Also, for historical reasons, this is called registerDB rather
        than register_wrapper.
        """

    def sortKey():
        """Sort key used to order distributed transactions

        When a transaction involved multiple storages, 2-phase commit
        operations are applied in sort-key order.  This must be unique
        among storages used in a transaction. Obviously, the storage
        can't assure this, but it should construct the sort key so it
        has a reasonable chance of being unique.

        The result must be a string.
        """

    def store(oid, serial, data, version, transaction):
        """Store data for the object id, oid.

        Arguments:

        oid
            The object identifier.  This is either a string
            consisting of 8 nulls or a string previously returned by
            new_oid.

        serial
            The serial of the data that was read when the object was
            loaded from the database.  If the object was created in
            the current transaction this will be a string consisting
            of 8 nulls.

        data
            The data record. This is opaque to the storage.

        version
            This must be an empty string. It exists for backward compatibility.

        transaction
            A transaction object.  This should match the current
            transaction for the storage, set by tpc_begin.

        The new serial for the object is returned, but not necessarily
        immediately.  It may be returned directly, or on a subsequent
        store or tpc_vote call.

        The return value may be:

        - None, or

        - A new serial (string) for the object

        If None is returned, then a new serial (or other special
        values) must ve returned in tpc_vote results.

        A serial, returned as a string, may be the special value
        ZODB.ConflictResolution.ResolvedSerial to indicate that a
        conflict occured and that the object should be invalidated.

        Several different exceptions may be raised when an error occurs.

        ConflictError
          is raised when serial does not match the most recent serial
          number for object oid and the conflict was not resolved by
          the storage.

        StorageTransactionError
          is raised when transaction does not match the current
          transaction.

        StorageError or, more often, a subclass of it
          is raised when an internal error occurs while the storage is
          handling the store() call.

        """

    def tpc_abort(transaction):
        """Abort the transaction.

        Any changes made by the transaction are discarded.

        This call is ignored is the storage is not participating in
        two-phase commit or if the given transaction is not the same
        as the transaction the storage is commiting.
        """

    def tpc_begin(transaction):
        """Begin the two-phase commit process.

        If storage is already participating in a two-phase commit
        using the same transaction, a StorageTransactionError is raised.

        If the storage is already participating in a two-phase commit
        using a different transaction, the call blocks until the
        current transaction ends (commits or aborts).
        """

    def tpc_finish(transaction, func = lambda tid: None):
        """Finish the transaction, making any transaction changes permanent.

        Changes must be made permanent at this point.

        This call raises a StorageTransactionError if the storage
        isn't participating in two-phase commit or if it is committing
        a different transaction.  Failure of this method is extremely
        serious.

        The second argument is a call-back function that must be
        called while the storage transaction lock is held.  It takes
        the new transaction id generated by the transaction.

        """

    def tpc_vote(transaction):
        """Provide a storage with an opportunity to veto a transaction

        This call raises a StorageTransactionError if the storage
        isn't participating in two-phase commit or if it is commiting
        a different transaction.

        If a transaction can be committed by a storage, then the
        method should return.  If a transaction cannot be committed,
        then an exception should be raised.  If this method returns
        without an error, then there must not be an error if
        tpc_finish or tpc_abort is called subsequently.

        The return value can be either None or a sequence of object-id
        and serial pairs giving new serials for objects who's ids were
        passed to previous store calls in the same transaction.
        After the tpc_vote call, new serials must have been returned,
        either from tpc_vote or store for objects passed to store.

        A serial returned in a sequence of oid/serial pairs, may be
        the special value ZODB.ConflictResolution.ResolvedSerial to
        indicate that a conflict occured and that the object should be
        invalidated.

        """

    def new_instance():
        """Creates and returns another storage instance.

        The returned instance provides IMVCCStorage and connects to the
        same back-end database. The database state visible by the
        instance will be a snapshot that varies independently of other
        storage instances.
        """

    def release():
        """Release all persistent sessions used by this storage instance.

        After this call, the storage instance can still be used;
        calling methods that use persistent sessions will cause the
        persistent sessions to be reopened.
        """

    def poll_invalidations():
        """Poll the storage for external changes.

        Returns either a sequence of OIDs that have changed, or None.  When a
        sequence is returned, the corresponding objects should be removed
        from the ZODB in-memory cache.  When None is returned, the storage is
        indicating that so much time has elapsed since the last poll that it
        is no longer possible to enumerate all of the changed OIDs, since the
        previous transaction seen by the connection has already been packed.
        In that case, the ZODB in-memory cache should be cleared.
        """

    def sync(force=True):
        """Updates the internal snapshot to the current state of the database.

        If the force parameter is False, the storage may choose to
        ignore this call. By ignoring this call, a storage can reduce
        the frequency of database polls, thus reducing database load.
        """


