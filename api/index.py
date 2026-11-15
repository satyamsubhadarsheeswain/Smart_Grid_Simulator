from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import asyncio
import random
import heapq
import time
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any

app = FastAPI()

@dataclass(order=True)
class PowerMessage:
    priority: int  # 1 = Critical Infrastructure, 2 = High (EV/Solar), 3 = Standard Residential
    timestamp: float = field(compare=True)
    msg_id: str = field(compare=False)
    home_id: str = field(compare=False)
    home_type: str = field(compare=False)
    appliance_surge: str = field(compare=False)
    requested_kw: float = field(compare=False)
    future_result: asyncio.Future = field(compare=False, default=None)

class CentralGridBroker:
    def __init__(self, capacity_kw: float):
        self.capacity_kw = capacity_kw
        self.priority_queue: List[PowerMessage] = []
        self._lock = asyncio.Lock()
        
        # Queuing Theory & Telemetry Metrics
        self.total_messages = 0
        self.total_wait_time = 0.0
        self.service_time_total = 0.0
        self.shed_count = 0
        self.curtailed_count = 0
        self.negotiation_logs: List[Dict[str, str]] = []

    def log_event(self, level: str, title: str, detail: str):
        self.negotiation_logs.append({
            "time": time.strftime("%H:%M:%S"),
            "level": level,  # INFO, PRIORITY, WARN, SHED
            "title": title,
            "detail": detail
        })

    async def publish_telemetry(self, msg_id: str, home_id: str, home_type: str, surge: str, requested_kw: float, priority: int) -> float:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        msg = PowerMessage(
            priority=priority,
            timestamp=time.time(),
            msg_id=msg_id,
            home_id=home_id,
            home_type=home_type,
            appliance_surge=surge,
            requested_kw=requested_kw,
            future_result=fut
        )
        async with self._lock:
            heapq.heappush(self.priority_queue, msg)
            self.total_messages += 1
        return await fut

    async def negotiate_and_schedule(self):
        """Priority-based message dequeuing and capacity negotiation algorithm."""
        process_start = time.time()
        async with self._lock:
            if not self.priority_queue:
                return
            ordered_messages = []
            while self.priority_queue:
                ordered_messages.append(heapq.heappop(self.priority_queue))

        total_demand = sum(m.requested_kw for m in ordered_messages)
        self.log_event(
            "INFO",
            "Broker Queue Snapshot",
            f"Ingested {len(ordered_messages)} node packets. Total Demand: {total_demand:.1f} kW vs Grid Cap: {self.capacity_kw:.1f} kW."
        )

        if total_demand > self.capacity_kw:
            deficit = total_demand - self.capacity_kw
            self.log_event(
                "WARN",
                "Grid Overload Detected",
                f"Demand exceeds capacity by {deficit:.1f} kW. Engaging Priority Heap Arbitration & Load Shedding."
            )

        remaining_kw = self.capacity_kw
        now = time.time()

        for msg in ordered_messages:
            wait_sec = max(0.0001, now - msg.timestamp)
            self.total_wait_time += wait_sec

            if remaining_kw >= msg.requested_kw:
                remaining_kw -= msg.requested_kw
                self.log_event(
                    "PRIORITY",
                    f"[P{msg.priority}] {msg.home_id} → 100% Allocated",
                    f"Granted {msg.requested_kw} kW ({msg.appliance_surge}). Remaining Grid Headroom: {remaining_kw:.1f} kW."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(msg.requested_kw)

            elif remaining_kw > 0:
                allocated = round(remaining_kw, 2)
                curtailed = round(msg.requested_kw - allocated, 2)
                remaining_kw = 0.0
                self.curtailed_count += 1
                self.log_event(
                    "WARN",
                    f"[P{msg.priority}] {msg.home_id} → Negotiated Curtailment",
                    f"Requested {msg.requested_kw} kW; throttled by {curtailed} kW. Allocated remaining {allocated} kW."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(allocated)

            else:
                self.shed_count += 1
                self.log_event(
                    "SHED",
                    f"[P{msg.priority}] {msg.home_id} → Load Shed (0 kW)",
                    f"Grid saturated (0.0 kW left). Deferred {msg.requested_kw} kW ({msg.appliance_surge}) to next cycle."
                )
                if not msg.future_result.done():
                    msg.future_result.set_result(0.0)

        self.service_time_total = max(0.001, time.time() - process_start)


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = os.path.join(os.path.dirname(__file__), "..", "public", "index.html")
    if not os.path.exists(html_path):
        html_path = "public/index.html"
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h3>Frontend public/index.html not found</h3>"


@app.get("/api/simulate")
async def run_simulation(
    capacity: float = Query(default=110.0, ge=20.0, le=500.0),
    surge_mode: str = Query(default="peak")
):
    broker = CentralGridBroker(capacity_kw=capacity)

    # Virtual Smart Home Nodes: (ID, Type, Base kW, Priority, Possible Surges)
    virtual_nodes = [
        ("NODE-01 • St. Jude Hospital", "Critical Medical", 24.0, 1, [("ICU Life Support", 6.0), ("MRI Scanner", 12.0)]),
        ("NODE-02 • Metro Fire & Rescue", "Emergency Ops", 14.0, 1, [("Dispatch Bay", 4.0), ("Fleet Charger", 8.0)]),
        ("NODE-03 • Villa Solaris", "Smart EV + Battery", 12.0, 2, [("Level-2 EV Charger", 14.0), ("Heat Pump HVAC", 6.0)]),
        ("NODE-04 • GreenTech Duplex", "Smart EV + Solar", 11.0, 2, [("Dual EV Fast Charge", 18.0), ("Base Load", 2.0)]),
        ("NODE-05 • Maple Residency", "Standard Home", 10.0, 3, [("Central AC + Dryer", 15.0), ("Evening Cooking", 7.0)]),
        ("NODE-06 • Oakwood Apt 4B", "Standard Home", 9.0, 3, [("Pool Heater + AC", 16.0), ("Normal Usage", 3.0)]),
        ("NODE-07 • Pinecrest Home", "Standard Home", 11.0, 3, [("Electric Water Heater", 12.0), ("AC Surge", 14.0)]),
        ("NODE-08 • Cedar Smart Loft", "Standard Home", 8.0, 3, [("Crypto/Home Lab", 15.0), ("Normal Usage", 2.5)]),
    ]

    sim_start = time.time()

    async def virtual_smart_home(idx: int, hid: str, htype: str, base_kw: float, prio: int, surges: list):
        # Asynchronous network arrival jitter
        await asyncio.sleep(random.uniform(0.002, 0.025))
        
        if surge_mode == "extreme":
            appliance, surge_kw = surges[0]
            surge_kw = round(surge_kw * 1.35, 1)
        elif surge_mode == "normal":
            appliance, surge_kw = surges[1]
        else:  # peak
            appliance, surge_kw = random.choice(surges)

        requested_kw = round(base_kw + surge_kw, 2)
        msg_id = f"MSG-{100 + idx}"

        enqueue_ts = time.time()
        allocated_kw = await broker.publish_telemetry(msg_id, hid, htype, appliance, requested_kw, prio)
        wait_ms = round((time.time() - enqueue_ts) * 1000, 2)

        if allocated_kw == requested_kw:
            status = "Allocated"
        elif allocated_kw > 0:
            status = "Curtailed"
        else:
            status = "Shed"

        return {
            "msg_id": msg_id,
            "home_id": hid,
            "home_type": htype,
            "priority": prio,
            "appliance": appliance,
            "requested_kw": requested_kw,
            "allocated_kw": allocated_kw,
            "curtailed_kw": round(requested_kw - allocated_kw, 2),
            "status": status,
            "wait_ms": wait_ms
        }

    # Spawn all virtual nodes concurrently
    tasks = [
        asyncio.create_task(virtual_smart_home(i, n[0], n[1], n[2], n[3], n[4]))
        for i, n in enumerate(virtual_nodes)
    ]

    # Allow packets to arrive in the priority heap queue
    await asyncio.sleep(0.04)
    await broker.negotiate_and_schedule()

    nodes_data = await asyncio.gather(*tasks)
    nodes_data.sort(key=lambda x: (x["priority"], x["msg_id"]))

    total_window = max(0.01, time.time() - sim_start)
    total_demanded = round(sum(n["requested_kw"] for n in nodes_data), 2)
    total_allocated = round(sum(n["allocated_kw"] for n in nodes_data), 2)
    grid_utilization = round((total_allocated / capacity) * 100, 1)
    demand_ratio = round((total_demanded / capacity) * 100, 1)

    # Queuing Theory Metrics (M/M/1 Approximation)
    arrival_rate_lambda = round(len(nodes_data) / total_window, 1)  # msgs/sec
    service_rate_mu = round(len(nodes_data) / broker.service_time_total, 1)  # msgs/sec
    avg_wait_ms = round((broker.total_wait_time / len(nodes_data)) * 1000, 2)

    return {
        "capacity_kw": capacity,
        "total_demanded_kw": total_demanded,
        "total_allocated_kw": total_allocated,
        "grid_utilization_pct": grid_utilization,
        "demand_ratio_pct": demand_ratio,
        "shed_count": broker.shed_count,
        "curtailed_count": broker.curtailed_count,
        "queuing_metrics": {
            "arrival_rate_lambda": arrival_rate_lambda,
            "service_rate_mu": service_rate_mu,
            "avg_wait_ms": avg_wait_ms,
            "queue_discipline": "Priority Heap (Min-Heap + FIFO)"
        },
        "nodes": nodes_data,
        "logs": broker.negotiation_logs
    }
