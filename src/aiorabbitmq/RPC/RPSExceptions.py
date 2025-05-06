class NoCorrelationIDException(Exception):
    pass


class RPCError(Exception):
    pass


__all__ = ["RPCError", "NoCorrelationIDException"]
