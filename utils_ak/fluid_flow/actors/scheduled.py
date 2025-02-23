from utils_ak.fluid_flow.actors.processor import Processor
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actor import Actor


class Scheduled(Actor):
    """
    A wrapper actor that schedules work for one actor while forcing another actor to update.

    The scheduled_actor is updated only if the provided schedule_func (the tumbler)
    returns True for the current time stamp. Regardless of the schedule, the forced_actor
    is always updated.

    Args:
        name (str): The name of this Scheduled actor.
        schedule_func (callable): A function that takes a timestamp (float) and returns a bool.
            When True, the scheduled_actor is active and will be updated.
        scheduled_actor (Actor): The actor whose update methods are controlled by the schedule.
        forced_actor (Actor): The actor that is always updated (forced to work) by this schedule.
    """

    def __init__(self, name: str, schedule_func, scheduled_actor: Actor, forced_actor: Actor):
        super().__init__(name)
        self.schedule_func = schedule_func
        self.scheduled_actor = scheduled_actor
        self.forced_actor = forced_actor

    # - Generic overrides

    def inner_actors(self):
        """Return both inner actors so that event manager propagation works."""
        return [self.scheduled_actor, self.forced_actor]

    def reset(self):
        """Reset self and the inner actors."""
        super().reset()
        self.scheduled_actor.reset()
        self.forced_actor.reset()

    # - Updaters

    def update_values(self, ts: float):
        if self.schedule_func(ts):
            self.scheduled_actor.update_values(ts)
        self.forced_actor.update_values(ts)

    def update_pressure(self, ts: float):
        if self.schedule_func(ts):
            self.scheduled_actor.update_pressure(ts)
        self.forced_actor.update_pressure(ts)

    def update_speed(self, ts: float):
        if self.schedule_func(ts):
            self.scheduled_actor.update_speed(ts)
        self.forced_actor.update_speed(ts)

    def update_triggers(self, ts: float):
        if self.schedule_func(ts):
            self.scheduled_actor.update_triggers(ts)
        self.forced_actor.update_triggers(ts)

    def update_last_ts(self, ts: float):
        self.last_ts = ts
        self.scheduled_actor.update_last_ts(ts)
        self.forced_actor.update_last_ts(ts)


    def __str__(self):
        return f"Scheduled ({self.name})"


def test():
    def my_tumbler(ts: float) -> bool:
        # For example, active only during even-numbered seconds
        return int(ts) % 2 == 0

    scheduled_processor = Processor("MyProcessor", lag=0.5, max_pressures=[10, None])
    forced_container = Container("MyForcedContainer", value=100)
    scheduler = Scheduled("MyScheduler", my_tumbler, scheduled_processor, forced_container)
