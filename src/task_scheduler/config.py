from dataclasses import dataclass


@dataclass(slots=True)
class SchedulerConfig:
    """Runtime configuration for one scheduler process."""

    max_concurrency: int = 4
    flush_interval: float = 10.0

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if self.flush_interval <= 0:
            raise ValueError("flush_interval must be positive")
