from abc import ABC, abstractmethod

class ET0Calculator(ABC):
    """
    Base class for ET0 (reference evapotranspiration) calculation.
    """

    registry: dict[str, type["ET0Calculator"]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "__abstractmethods__", None):
            ET0Calculator.registry[cls.__name__] = cls

    @property
    @abstractmethod
    def name(self):
        pass    

    @abstractmethod
    def calculate(self, data):
        pass

    def get_calculator_by_name(self, name):
        return self.registry.get(name)

