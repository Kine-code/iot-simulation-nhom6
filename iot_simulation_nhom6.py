"""Mini-lab: discrete-event simulation of an IoT uplink.

The model represents periodic sensor messages entering a bounded gateway queue.
Two abstract delivery profiles are compared:
- best_effort: one transmission attempt, no application acknowledgement.
- confirmed: acknowledgement delay and at most three transmission attempts.

This is an educational model, not a bit-accurate implementation of MQTT or CoAP.
"""
from __future__ import annotations

import argparse
import heapq
import math
import random
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List

import matplotlib.pyplot as plt
import pandas as pd

# ============================================================================
# KHU VUC CAU HINH DEMO - CHI CAN SUA CAC BIEN TRONG KHUNG NAY
# ============================================================================
# MUC DICH:
#   Chu dong thay doi 5 nhom tham so khi trinh bay demo mo phong:
#   (1) So luong thiet bi
#   (2) Xac suat mat goi / do tin cay
#   (3) Do tre xu ly va do tre ACK
#   (4) Toc do duong truyen
#   (5) Co che truyen lai va kich thuoc hang doi
#
# LUU Y QUAN TRONG:
#   - Day la MO PHONG giao tiep IoT, khong gui du lieu that len ThingsBoard.
#   - Don vi thoi gian la GIAY. Vi du 0.020 s = 20 ms.
#   - Sau khi sua tham so, luu file va chay lai lenh:
#       python iot_simulation_annotated.py --output-dir ket_qua_ss16 --seeds 20
# ============================================================================

# ---------------------------------------------------------------------------
# 1. SO LUONG THIET BI - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# Danh sach cac muc tai duoc dua len bieu do.
# Moi gia tri la so thiet bi IoT gui du lieu cung luc vao gateway.
#
# Vi du tai nhe:
#   DEVICE_COUNTS = [1, 10, 20, 50]
# Vi du tai vua:
#   DEVICE_COUNTS = [20, 50, 80, 100, 120, 150, 200]
# Vi du tai cao:
#   DEVICE_COUNTS = [100, 200, 300, 500, 1000]
#
# Anh huong:
#   So thiet bi tang -> so ban tin tang -> hang doi dai hon -> do tre tang.
#   Neu hang doi day -> ban tin bi loai bo -> do tin cay giam.
DEVICE_COUNTS = [20, 50, 80, 100, 120, 150, 200]  # <== SUA SO THIET BI O DAY

# ---------------------------------------------------------------------------
# 2. XAC SUAT MAT GOI VA DO TIN CAY - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# LOSS_PROBABILITY la xac suat that bai cua MOI LAN TRUYEN.
# Do tin cay ly thuyet moi lan truyen = 1 - LOSS_PROBABILITY.
#
# Vi du:
#   0.01 -> mat 1%  -> tin cay 99%
#   0.05 -> mat 5%  -> tin cay 95%
#   0.10 -> mat 10% -> tin cay 90%
#   0.20 -> mat 20% -> tin cay 80%
#
# LOSS_PROBABILITY duoc dung trong thi nghiem tang so thiet bi.
LOSS_PROBABILITY = 0.05  # <== SUA DO TIN CAY MAC DINH O DAY

# Danh sach cac muc mat goi dung de tao hai bieu do loss_delivery va loss_latency.
# Muon so sanh 99%, 97%, 95%, 90% thi dung [0.01, 0.03, 0.05, 0.10].
LOSS_PROBABILITIES = [0.01, 0.05, 0.10, 0.20]  # <== SUA CAC MUC TIN CAY O DAY

# ---------------------------------------------------------------------------
# 3. DO TRE XU LY VA DO TRE XAC NHAN ACK - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# MAC_PROCESSING_S: do tre xu ly moi lan gateway/server phuc vu mot goi.
#   0.010 = 10 ms; 0.020 = 20 ms; 0.100 = 100 ms.
# Gia tri tang -> moi goi chiem may chu lau hon -> hang doi tang -> do tre tang.
MAC_PROCESSING_S = 0.020  # <== SUA DO TRE XU LY O DAY

# ACK_DELAY_S: do tre phan hoi xac nhan, chi cong vao che do confirmed.
#   0.020 = 20 ms; 0.100 = 100 ms; 0.200 = 200 ms.
# Gia tri tang -> confirmed cham hon, nhung van co kha nang truyen lai.
ACK_DELAY_S = 0.020  # <== SUA DO TRE ACK O DAY

# BASE_BACKOFF_S: thoi gian cho co so truoc khi gui lai sau mot lan that bai.
# Chuong trinh dung backoff tang dan theo so lan thu.
#   0.050 = 50 ms; 0.100 = 100 ms; 0.500 = 500 ms.
# Gia tri tang -> giam nguy co gui lai dong loat, nhung do tre tong tang.
BASE_BACKOFF_S = 0.100  # <== SUA DO TRE CHO TRUYEN LAI O DAY

# ---------------------------------------------------------------------------
# 4. TOC DO DUONG TRUYEN - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# EFFECTIVE_BITRATE_BPS la toc do hieu dung tinh bang bit/giay.
#   10_000  = mang cham
#   50_000  = mac dinh
#   100_000 = mang nhanh hon
#
# Cong thuc thoi gian phuc vu mot goi:
#   service_time = ((payload + header) * 8 / bitrate) + processing_delay
#
# Bitrate giam -> thoi gian truyen tang -> hang doi tang -> do tre va mat goi tang.
EFFECTIVE_BITRATE_BPS = 50_000  # <== SUA TOC DO DUONG TRUYEN O DAY

# ---------------------------------------------------------------------------
# 5. CO CHE TRUYEN LAI - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# Chuong trinh luon so sanh hai che do:
#   best_effort: gui mot lan, khong ACK, khong gui lai.
#   confirmed: co ACK va duoc thu lai toi da MAX_ATTEMPTS lan.
#
# MAX_ATTEMPTS la TONG so lan thu, bao gom lan dau tien:
#   1 = khong truyen lai
#   3 = lan dau + toi da 2 lan truyen lai
#   5 = lan dau + toi da 4 lan truyen lai
#
# Tang MAX_ATTEMPTS thuong lam tang ty le nhan cua confirmed,
# nhung cung lam tang tai mang, do tre va nguy co day hang doi.
MAX_ATTEMPTS = 3  # <== SUA SO LAN THU TOI DA O DAY

# ---------------------------------------------------------------------------
# 6. CHU KY GUI DU LIEU - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# Moi thiet bi tao mot ban tin sau moi khoang thoi gian nay.
#   10.0 = moi 10 giay
#    5.0 = moi 5 giay
#    1.0 = moi 1 giay, tai cao hon 5 lan so voi 5 giay
#    0.5 = moi 0.5 giay, tai cao hon 10 lan so voi 5 giay
# Chu ky giam -> so ban tin/giay tang -> do tre va mat goi co the tang.
GENERATION_INTERVAL_S = 5.0  # <== SUA CHU KY GUI O DAY

# ---------------------------------------------------------------------------
# 7. KICH THUOC HANG DOI - THAY DOI TAI DAY
# ---------------------------------------------------------------------------
# So goi toi da co the cho xu ly tai gateway.
#   20  = hang doi nho, de rot goi
#   100 = mac dinh
#   500 = hang doi lon, giam rot goi nhung co the tang thoi gian cho
QUEUE_CAPACITY_PACKETS = 100  # <== SUA KICH THUOC HANG DOI O DAY

# ---------------------------------------------------------------------------
# 8. THOI GIAN MO PHONG MOI LAN CHAY
# ---------------------------------------------------------------------------
# 300 giay = 5 phut thoi gian mo phong cho moi cau hinh.
# Day la thoi gian trong mo hinh, khong phai bat buoc doi dung 5 phut ngoai doi.
DURATION_S = 300.0  # <== SUA THOI GIAN MO PHONG O DAY

# ---------------------------------------------------------------------------
# 9. KICH THUOC BAN TIN - CO THE SUA NEU CAN
# ---------------------------------------------------------------------------
# PAYLOAD_BYTES: du lieu cam bien thuc te.
# HEADER_BYTES: phan dau giao thuc uoc luong.
# Ban tin lon hon -> mat nhieu thoi gian truyen hon.
PAYLOAD_BYTES = 128  # <== SUA KICH THUOC DU LIEU O DAY
HEADER_BYTES = 40    # <== SUA KICH THUOC HEADER O DAY

# ============================================================================
# GOI Y 3 KICH BAN DEMO
# ============================================================================
# MANG TOT:
#   LOSS_PROBABILITY = 0.01
#   MAC_PROCESSING_S = 0.010
#   ACK_DELAY_S = 0.010
#   BASE_BACKOFF_S = 0.050
#   EFFECTIVE_BITRATE_BPS = 100_000
#
# MANG TRUNG BINH - CAU HINH MAC DINH CUA BAO CAO:
#   LOSS_PROBABILITY = 0.05
#   MAC_PROCESSING_S = 0.020
#   ACK_DELAY_S = 0.020
#   BASE_BACKOFF_S = 0.100
#   EFFECTIVE_BITRATE_BPS = 50_000
#
# MANG XAU / TAI CAO:
#   LOSS_PROBABILITY = 0.20
#   MAC_PROCESSING_S = 0.100
#   ACK_DELAY_S = 0.100
#   BASE_BACKOFF_S = 0.500
#   EFFECTIVE_BITRATE_BPS = 10_000
#   GENERATION_INTERVAL_S = 1.0
#   QUEUE_CAPACITY_PACKETS = 50
# ============================================================================


@dataclass(order=True)
class Event:
    time: float
    order: int
    kind: str = field(compare=False)
    packet_id: int = field(compare=False, default=-1)


@dataclass
class Packet:
    packet_id: int
    generated_time: float
    attempts: int = 0
    terminal: bool = False


def percentile(values: List[float], percentile_value: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile_value / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def simulate(
    num_devices: int = 100,
    mode: str = "best_effort",
    loss_probability: float = LOSS_PROBABILITY,  # <-- DOI DO TIN CAY O DAY
    duration_s: float = DURATION_S,
    generation_interval_s: float = GENERATION_INTERVAL_S,  # <-- DOI CHU KY GUI
    payload_bytes: int = 128,
    header_bytes: int = 40,
    effective_bitrate_bps: int = EFFECTIVE_BITRATE_BPS,  # <-- DOI TOC DO/TRI HOAN
    mac_processing_s: float = MAC_PROCESSING_S,  # <-- DOI TRE XU LY
    queue_capacity_packets: int = QUEUE_CAPACITY_PACKETS,
    max_attempts: int = MAX_ATTEMPTS,  # <-- DOI SO LAN TRUYEN LAI
    acknowledgement_delay_s: float = ACK_DELAY_S,  # <-- DOI TRE ACK
    base_backoff_s: float = BASE_BACKOFF_S,  # <-- DOI TRE TRUYEN LAI
    seed: int = 1,
) -> Dict[str, float]:
    """Run one simulation and return aggregate metrics."""
    if mode not in {"best_effort", "confirmed"}:
        raise ValueError("mode must be 'best_effort' or 'confirmed'")
    if not 0.0 <= loss_probability < 1.0:
        raise ValueError("loss_probability must be in [0, 1)")

    rng = random.Random(seed)
    service_time_s = (
        (payload_bytes + header_bytes) * 8 / effective_bitrate_bps
        + mac_processing_s
    )

    events: List[Event] = []
    event_counter = 0

    def schedule(time_s: float, kind: str, packet_id: int = -1) -> None:
        nonlocal event_counter
        event_counter += 1
        heapq.heappush(events, Event(time_s, event_counter, kind, packet_id))

    packets: Dict[int, Packet] = {}
    packet_id = 0
    for _device in range(num_devices):
        time_s = rng.uniform(0.0, generation_interval_s)
        while time_s < duration_s:
            packets[packet_id] = Packet(packet_id, time_s)
            schedule(time_s, "arrival", packet_id)
            packet_id += 1
            jitter = rng.uniform(
                -0.15 * generation_interval_s,
                0.15 * generation_interval_s,
            )
            time_s += max(0.05, generation_interval_s + jitter)

    generated = packet_id
    queue: List[tuple[int, float]] = []
    server_busy = False

    delivered_latencies_s: List[float] = []
    queue_waits_s: List[float] = []
    terminal_losses = 0
    queue_drops = 0
    wireless_failures = 0
    total_attempts = 0
    retransmissions = 0
    max_queue_length = 0

    def start_next(now_s: float) -> None:
        nonlocal server_busy
        if queue and not server_busy:
            queued_packet_id, enqueued_at_s = queue.pop(0)
            queue_waits_s.append(now_s - enqueued_at_s)
            server_busy = True
            schedule(now_s + service_time_s, "service_done", queued_packet_id)

    while events:
        event = heapq.heappop(events)
        now_s = event.time
        packet = packets[event.packet_id] if event.packet_id >= 0 else None

        if event.kind == "arrival":
            if packet is None or packet.terminal:
                continue
            if len(queue) >= queue_capacity_packets:
                queue_drops += 1
                # A message that never entered the queue cannot benefit from a link retry.
                if packet.attempts == 0 or mode == "best_effort":
                    packet.terminal = True
                    terminal_losses += 1
                else:
                    delay_s = (
                        base_backoff_s * (2 ** max(0, packet.attempts - 1))
                        + rng.uniform(0.0, base_backoff_s)
                    )
                    schedule(now_s + delay_s, "arrival", packet.packet_id)
            else:
                queue.append((packet.packet_id, now_s))
                max_queue_length = max(max_queue_length, len(queue))
                start_next(now_s)

        elif event.kind == "service_done":
            server_busy = False
            if packet is None or packet.terminal:
                start_next(now_s)
                continue

            packet.attempts += 1
            total_attempts += 1
            transmission_succeeded = rng.random() >= loss_probability

            if transmission_succeeded:
                packet.terminal = True
                latency_s = now_s - packet.generated_time
                if mode == "confirmed":
                    latency_s += acknowledgement_delay_s
                delivered_latencies_s.append(latency_s)
            else:
                wireless_failures += 1
                if mode == "confirmed" and packet.attempts < max_attempts:
                    retransmissions += 1
                    delay_s = (
                        base_backoff_s * (2 ** (packet.attempts - 1))
                        + rng.uniform(0.0, base_backoff_s)
                    )
                    schedule(now_s + delay_s, "arrival", packet.packet_id)
                else:
                    packet.terminal = True
                    terminal_losses += 1
            start_next(now_s)

    delivered = len(delivered_latencies_s)
    return {
        "generated": generated,
        "delivered": delivered,
        "delivery_ratio": delivered / generated if generated else 0.0,
        "mean_latency_ms": (
            statistics.fmean(delivered_latencies_s) * 1000
            if delivered_latencies_s
            else float("nan")
        ),
        "p95_latency_ms": percentile(delivered_latencies_s, 95) * 1000,
        "goodput_messages_per_s": delivered / duration_s,
        "queue_drop_ratio": queue_drops / generated if generated else 0.0,
        "mean_attempts_per_generated": (
            total_attempts / generated if generated else 0.0
        ),
        "retransmission_ratio_per_attempt": (
            retransmissions / total_attempts if total_attempts else 0.0
        ),
        "wireless_failure_ratio_per_attempt": (
            wireless_failures / total_attempts if total_attempts else 0.0
        ),
        "mean_queue_wait_ms": (
            statistics.fmean(queue_waits_s) * 1000 if queue_waits_s else 0.0
        ),
        "max_queue_length": max_queue_length,
        "service_time_ms": service_time_s * 1000,
        "terminal_losses": terminal_losses,
        "queue_drops": queue_drops,
        "total_attempts": total_attempts,
    }


def ci95(values: Iterable[float]) -> float:
    values = list(values)
    if len(values) < 2:
        return 0.0
    return 1.96 * statistics.stdev(values) / math.sqrt(len(values))


def aggregate_runs(rows: pd.DataFrame, group_columns: List[str]) -> pd.DataFrame:
    metric_columns = [
        "delivery_ratio",
        "mean_latency_ms",
        "p95_latency_ms",
        "goodput_messages_per_s",
        "queue_drop_ratio",
        "mean_attempts_per_generated",
        "mean_queue_wait_ms",
    ]
    output_rows = []
    for keys, group in rows.groupby(group_columns, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_columns, keys))
        for metric in metric_columns:
            values = group[metric].tolist()
            row[metric] = statistics.fmean(values)
            row[f"{metric}_ci95"] = ci95(values)
        output_rows.append(row)
    return pd.DataFrame(output_rows)


def run_experiments(output_dir: Path, seeds: int = 20) -> tuple[pd.DataFrame, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_rows = []

    device_counts = DEVICE_COUNTS  # <-- DOI TAI HE THONG O DAU FILE
    modes = ["best_effort", "confirmed"]
    for num_devices in device_counts:
        for mode in modes:
            for run_index in range(seeds):
                metrics = simulate(
                    num_devices=num_devices,
                    mode=mode,
                    loss_probability=LOSS_PROBABILITY,  # <-- DO TIN CAY MAC DINH
                    seed=10_000 + run_index,
                )
                raw_rows.append(
                    {
                        "experiment": "scaling",
                        "num_devices": num_devices,
                        "loss_probability": 0.05,
                        "mode": mode,
                        "run": run_index,
                        **metrics,
                    }
                )

    loss_probabilities = LOSS_PROBABILITIES  # <-- CAC MUC TIN CAY DE SO SANH
    for loss_probability in loss_probabilities:
        for mode in modes:
            for run_index in range(seeds):
                metrics = simulate(
                    num_devices=100,
                    mode=mode,
                    loss_probability=loss_probability,
                    seed=20_000 + run_index,
                )
                raw_rows.append(
                    {
                        "experiment": "loss_sweep",
                        "num_devices": 100,
                        "loss_probability": loss_probability,
                        "mode": mode,
                        "run": run_index,
                        **metrics,
                    }
                )

    raw = pd.DataFrame(raw_rows)
    scaling = aggregate_runs(
        raw[raw["experiment"] == "scaling"],
        ["num_devices", "mode"],
    )
    loss_sweep = aggregate_runs(
        raw[raw["experiment"] == "loss_sweep"],
        ["loss_probability", "mode"],
    )

    raw.to_csv(output_dir / "raw_runs.csv", index=False)
    scaling.to_csv(output_dir / "scaling_summary.csv", index=False)
    loss_sweep.to_csv(output_dir / "loss_summary.csv", index=False)
    create_plots(scaling, loss_sweep, output_dir)
    return scaling, loss_sweep


def create_plots(scaling: pd.DataFrame, loss_sweep: pd.DataFrame, output_dir: Path) -> None:
    labels = {"best_effort": "Best-effort", "confirmed": "Confirmed"}

    def plot_by_mode(data, x, y, xlabel, ylabel, filename, yscale=None):
        fig, ax = plt.subplots(figsize=(7.2, 4.5))
        for mode in ["best_effort", "confirmed"]:
            part = data[data["mode"] == mode].sort_values(x)
            ax.plot(part[x], part[y], marker="o", label=labels[mode])
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        if yscale:
            ax.set_yscale(yscale)
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(output_dir / filename, dpi=220, bbox_inches="tight")
        plt.close(fig)

    plot_by_mode(
        scaling,
        "num_devices",
        "delivery_ratio",
        "Số thiết bị",
        "Tỷ lệ chuyển giao thành công",
        "scaling_delivery.png",
    )
    plot_by_mode(
        scaling,
        "num_devices",
        "p95_latency_ms",
        "Số thiết bị",
        "Độ trễ P95 (ms, thang log)",
        "scaling_latency.png",
        yscale="log",
    )
    plot_by_mode(
        scaling,
        "num_devices",
        "queue_drop_ratio",
        "Số thiết bị",
        "Tỷ lệ loại bỏ do đầy hàng đợi",
        "scaling_queue_drop.png",
    )
    plot_by_mode(
        loss_sweep,
        "loss_probability",
        "delivery_ratio",
        "Xác suất mất gói mỗi lần truyền",
        "Tỷ lệ chuyển giao thành công",
        "loss_delivery.png",
    )
    plot_by_mode(
        loss_sweep,
        "loss_probability",
        "p95_latency_ms",
        "Xác suất mất gói mỗi lần truyền",
        "Độ trễ P95 (ms, thang log)",
        "loss_latency.png",
        yscale="log",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("iot_simulation_results"),
        help="Directory for CSV files and plots.",
    )
    parser.add_argument(
        "--seeds",
        type=int,
        default=20,
        help="Number of independent random seeds for each scenario.",
    )
    args = parser.parse_args()
    scaling, loss_sweep = run_experiments(args.output_dir, seeds=args.seeds)
    print("Scaling experiment")
    print(scaling.to_string(index=False))
    print("\nLoss experiment")
    print(loss_sweep.to_string(index=False))
    print(f"\nResults written to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
