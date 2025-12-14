import pandas as pd

from dataclasses import dataclass

from .database.models import Field
from .irrigation import FieldIrrigation

@dataclass
class FieldCapacity:
    soil_type: str
    root_dept: float
    humus_pct: float
    nfk_mm_per_dm: float
    nfk_total_mm: float

    def __post_init(self):
        if self.root_dept <= 0:
            raise ValueError("root_depth_cm must be > 0.")
        if self.nfk_mm_per_dm <= 0:
            raise ValueError("nfk_mm_per_dm must be > 0.")
        if self.nfk_total_mm <= 0:
            raise ValueError("nfk_total_mm must be > 0.")
        if self.humus_pct < 0:
            raise ValueError("humus_pct cannot be negative.")

class FieldHandler:

    def __init__(self, field: Field):
        self.id = field.id
        self.name = field.name
        self.reference_station = field.reference_station
        self.soil_type = field.soil_type
        self.humus_pct = field.humus_pct
        self.area_ha = field.area_ha
        self.root_depth_cm = field.root_depth_cm
        self.p_allowable = field.p_allowable
        self.field_capacity: FieldCapacity | None = None
        self.water_balance: pd.DataFrame | None = None

    def get_field_capacity(
        self,
        custom_lookup: dict | None = None
    ) -> FieldCapacity:
        """
        Schätzt die Feldkapazität als nutzbare Feldkapazität (nFK) über die Wurzelzone.

        Methode:
          1) Bodenart → nFK-Bereich (mm/dm) aus Lookup.
          2) Mittelwert.
          3) Humus-Aufschlag: +1.5 mm/dm je 1% Humus über 1.5%, max +6 mm/dm.
          4) nFK_total = nFK_mm_pro_dm * (Wurzeltiefe in dm).
          5) Optional: RAW = p_allowable * nFK_total.

        Args:
            root_depth_cm: Wurzeltiefe der Kultur in cm.
            custom_lookup: Optional eigene Mapping-Tabelle
                {bodenart_lower: (min_mm_pro_dm, max_mm_pro_dm)}.

        Returns:
            dict mit:
              - "soil_type": Bodenart (Original)
              - "nfk_mm_per_dm": nFK in mm/dm (inkl. Humus-Aufschlag)
              - "nfk_total_mm": nFK über Wurzeltiefe in mm
              - "raw_mm": readily available water (falls p_allowable gesetzt)
              - "assumptions": getroffene Annahmen (Humus, Tiefe, Quelle)
        Raises:
            KeyError: wenn Bodenart nicht in der Lookup-Tabelle ist.
            ValueError: bei unplausiblen Eingaben.
        """

        # Standard-Lookup
        default_lookup: dict[str, tuple[float, float]] = {
            # sehr sandig
            "sand": (6, 12),
            "schwach lehmiger sand": (8, 14),
            "lehmiger sand": (12, 18),
            "schluffiger sand": (10, 16),
            # schluff/loam
            "sandiger schluff": (20, 28),
            "schluff": (22, 30),
            "lehm": (18, 25),
            "sandiger lehm": (16, 22),
            "schluffiger lehm": (20, 28),
            # tonig
            "toniger lehm": (18, 26),
            "schluffiger ton": (18, 25),
            "ton": (15, 22),
            # organisch angereichert
            "humoser lehmiger sand": (14, 20)
        }

        lookup = custom_lookup or default_lookup

        if self.soil_type.lower() not in lookup:
            raise KeyError(
                f"Bodenart '{self.soil_type}' not found in Lookup table. "
                "Use 'custom_lookup' argument or add to default table in FieldHandler."
            )

        nfk_min, nfk_max = lookup[self.soil_type.lower()]
        base_mm_per_dm = (nfk_min + nfk_max) / 2.0

        # Humus-Aufschlag: +1.5 mm/dm je 1% über 1.5%, max +6 mm/dm
        humus_extra = max(0.0, self.humus_pct - 1.5) * 1.5
        humus_extra = min(humus_extra, 6.0)

        nfk_mm_per_dm = base_mm_per_dm + humus_extra
        nfk_total_mm = nfk_mm_per_dm * (self.root_depth_cm / 10.0)

        capacity = FieldCapacity(
            soil_type=self.soil_type,
            root_dept = self.root_depth_cm,   
            humus_pct = self.humus_pct,         
            nfk_mm_per_dm=nfk_mm_per_dm,
            nfk_total_mm=nfk_total_mm,
        )

        self.field_capacity = capacity
        return capacity

    def calculate_water_balance(
        self,
        station_data: pd.DataFrame,
        field_irrigation: FieldIrrigation | None = None,
        initial_storage: float | None = None,
    ):
        """
        Calculate the daily water balance of the field. The water balance is defined as incoming water (precipitation + irrigation)
        minus the actual evapotranspiration during a particular day. The maximum water balance corresponds to the soil field capacity of
        the field.
        """
        if station_data is None or station_data.empty:
            raise ValueError("Station data cannot be empty when calculating the water balance.")

        if not isinstance(station_data.index, pd.DatetimeIndex):
            raise TypeError("Station data index must be a pandas DatetimeIndex.")

        if self.field_capacity is None:
            raise ValueError("Field capacity unknown. Call get_field_capacity() beforehand.")

        data = station_data.sort_index().copy()

        if "precipitation" not in data.columns:
            raise KeyError("Station data must contain a 'precipitation' column.")

        et_column = "et0_corrected" if "et0_corrected" in data.columns else "et0" if "et0" in data.columns else None
        if et_column is None:
            raise KeyError("Station data must contain either 'et0_corrected' or 'et0'.")

        precip = data["precipitation"].fillna(0.0)
        evap = data[et_column].fillna(0.0)

        if field_irrigation is None:
            irrigation = pd.Series(0.0, index=data.index)
        else:
            irrigation = field_irrigation.to_dataframe(data.index, fill_value = 0.0)

        incoming = precip + irrigation
        net = incoming - evap

        capacity = self.field_capacity.nfk_total_mm

        storage = []
        current_storage = capacity if initial_storage is None else max(0.0, min(capacity, initial_storage))
        for delta in net:
            current_storage = max(0.0, min(capacity, current_storage + delta))
            storage.append(current_storage)

        water_balance = pd.DataFrame(
            {
                "precipitation": precip,
                "irrigation": irrigation,
                "evapotranspiration": evap,
                "incoming": incoming,
                "net": net,
                "soil_storage": storage,
            },
            index=data.index,
        )
        water_balance["field_capacity"] = capacity
        water_balance["deficit"] = capacity - water_balance["soil_storage"]
        water_balance["field_id"] = self.id

        if self.p_allowable:
            raw = self.p_allowable * capacity
            trigger_level = capacity - raw
            water_balance["readily_available_water"] = raw
            water_balance["below_raw"] = water_balance["soil_storage"] < trigger_level

        self.water_balance = water_balance
        return water_balance
