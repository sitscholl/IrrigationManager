from abc import ABC, abstractmethod

class ET0Calculator(ABC):
    """
    Base class for ET0 (reference evapotranspiration) calculation.
    """

    registry: dict[str, type["ET0Calculator"]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "__abstractmethods__", None):
            ET0Calculator.registry[cls.name()] = cls

    @classmethod
    @abstractmethod
    def name(cls):
        pass    

    @abstractmethod
    def calculate(self, data):
        pass

    def get_calculator_by_name(name):
        return ET0Calculator.registry.get(name)

