from DAQ.util.config import get_redis_conn

def set_fault(mac: str, fault: str):
    r = get_redis_conn(db=3)
    mac = mac.lower()
    r.set(f"fault_injection:{mac}", fault)

def get_fault(mac: str) -> str:
    r = get_redis_conn(db=3)
    mac = mac.lower()
    val = r.get(f"fault_injection:{mac}")
    if val:
        return val
    return "normal"

def reset_fault(mac: str):
    r = get_redis_conn(db=3)
    mac = mac.lower()
    r.delete(f"fault_injection:{mac}")
