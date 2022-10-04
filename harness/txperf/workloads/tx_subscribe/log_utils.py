from enum import Enum

class State(Enum):
    INIT = 0
    STARTED = 1
    CONSTRUCTING = 2
    CONSTRUCTED = 3
    TX = 4
    COMMIT = 5
    OK = 6
    ERROR = 7
    EVENT = 8
    ABORT = 9
    LOG = 10
    P1000 = 11

cmds = {
    "started": State.STARTED,
    "constructing": State.CONSTRUCTING,
    "constructed": State.CONSTRUCTED,
    "tx": State.TX,
    "cmt": State.COMMIT,
    "brt": State.ABORT,
    "ok": State.OK,
    "err": State.ERROR,
    "event": State.EVENT,
    "log": State.LOG,
    "+1000": State.P1000
}

threads = {
    "producing": {
        State.STARTED: [State.CONSTRUCTING],
        State.CONSTRUCTING: [State.CONSTRUCTED, State.ERROR],
        State.CONSTRUCTED: [State.CONSTRUCTING, State.P1000],
        State.P1000: [State.P1000, State.ERROR, State.CONSTRUCTING],
        State.ERROR: [State.CONSTRUCTING]
    },
    "streaming": {
        State.STARTED: [State.CONSTRUCTING],
        State.CONSTRUCTING: [State.CONSTRUCTED, State.ERROR],
        State.CONSTRUCTED: [State.CONSTRUCTING, State.TX],
        State.TX: [State.ABORT, State.COMMIT, State.ERROR],
        State.ABORT: [State.OK, State.ERROR],
        State.COMMIT: [State.OK, State.ERROR],
        State.OK: [State.TX],
        State.ERROR: [State.TX]
    }
}

phantoms = [ State.EVENT, State.LOG ]