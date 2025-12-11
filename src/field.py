import pandas as pd

from dataclasses import dataclass

from .database.models import Field

@dataclass
class FieldCapacity:
    soil_type: str
    root_dept: float
    humus_pct: float
    nfk_mm_per_dm: float
    nfk_total_mm: float

class FieldHandler:

    def __init__(self, field: Field):
        self.id = field.id
        self.name = field.name
        self.reference_station = field.reference_station
        self.soil_type = field.soil_type
        self.area_ha = field.area_ha
        self.p_allowable = field.p_allowable
        self.field_capacity: FieldCapacity | None = None
        self.water_balance: pd.DataFrame | None = None

    def get_field_capacity(
        self,
        humus_pct: float,
        root_depth_cm: float = 30.0,
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
            humus_pct: Humusgehalt in %. (Faustregel-Aufschlag aktiviert >1.5 %)
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

        if root_depth_cm <= 0:
            raise ValueError("root_depth_cm must be > 0.")
        if humus_pct < 0:
            raise ValueError("humus_pct cannot be negative.")

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
        humus_extra = max(0.0, humus_pct - 1.5) * 1.5
        humus_extra = min(humus_extra, 6.0)

        nfk_mm_per_dm = base_mm_per_dm + humus_extra
        nfk_total_mm = nfk_mm_per_dm * (root_depth_cm / 10.0)

        capacity = FieldCapacity(
            soil_type=self.soil_type,
            root_dept = root_depth_cm,   
            humus_pct = humus_pct,         
            nfk_mm_per_dm=nfk_mm_per_dm,
            nfk_total_mm=nfk_total_mm,
        )

        self.field_capacity = capacity
        return capacity

    def calculate_water_balance(self, station_data: pd.DataFrame, irrigation_events: pd.DataFrame):
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

        irrigation = pd.Series(0.0, index=data.index, name="irrigation")
        if irrigation_events is not None and not irrigation_events.empty:
            irr_df = irrigation_events.copy()
            amount_column = next((col for col in ("amount", "amount_mm", "depth_mm", "depth") if col in irr_df.columns), None)
            if amount_column is None:
                raise KeyError("Irrigation events dataframe must contain an amount column (amount, amount_mm, depth_mm, depth).")

            if "date" in irr_df.columns:
                irr_df["date"] = pd.to_datetime(irr_df["date"])
                irr_df = irr_df.set_index("date")
            elif not isinstance(irr_df.index, pd.DatetimeIndex):
                raise TypeError("Irrigation events require a datetime index or a 'date' column.")

            irr_df = irr_df.sort_index()
            target_tz = data.index.tz
            if target_tz is not None:
                if irr_df.index.tz is None:
                    irr_df.index = irr_df.index.tz_localize(target_tz)
                else:
                    irr_df.index = irr_df.index.tz_convert(target_tz)
            elif irr_df.index.tz is not None:
                irr_df.index = irr_df.index.tz_localize(None)

            daily = irr_df[amount_column].fillna(0.0).groupby(irr_df.index.normalize()).sum()
            aligned = daily.reindex(data.index.normalize(), fill_value=0.0)
            aligned.index = data.index
            irrigation = aligned.astype(float)

        incoming = precip + irrigation
        net = incoming - evap

        capacity = self.field_capacity.nfk_total_mm
        if capacity <= 0:
            raise ValueError("Field capacity must be greater than zero to calculate the water balance.")

        storage = []
        current_storage = capacity
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

        if self.p_allowable:
            raw = self.p_allowable * capacity
            trigger_level = capacity - raw
            water_balance["readily_available_water"] = raw
            water_balance["below_raw"] = water_balance["soil_storage"] < trigger_level

        self.water_balance = water_balance
        return water_balance
