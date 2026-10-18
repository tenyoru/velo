pub const Wait = enum {
    idle,
    waiting,
    granted,
    canceled,
    done,
};

pub const Channel = enum {
    idle,
    waiting,
    granted,
    canceled,
    closed,
    done,
};

pub const Task = enum {
    idle,
    scheduled,
    done,
};

pub const Operation = enum {
    idle,
    queued,
    inflight,
    complete,
    done,
};

pub const Job = enum {
    idle,
    pending,
    complete,
    done,
};
