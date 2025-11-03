from dataclasses import dataclass

from database.models import Field

@dataclass
class FieldCapacity:
    soil_type: str
    root_dept: float
    humus_pct: float
    nfk_mm_per_dm: float
    nfk_total_mm: float

class FieldHandler:

    def __init__(self, field: Field, irrigation_manager):
        self.id = field.id
        self.name = field.name
        self.reference_station = field.reference_station
        self.soil_type = field.soil_type
        self.area_ha = field.area_ha
        self.p_allowable = field.p_allowable

    def get_field_capacity(
        self,
        humus_pct: float,
        root_depth_cm: float = 30.0,
        custom_lookup: dict | None = None
    ) -> dict:
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

        return FieldCapacity(
            soil_type=self.soil_type,
            root_dept = root_depth_cm,   
            humus_pct = humus_pct,         
            nfk_mm_per_dm=nfk_mm_per_dm,
            nfk_total_mm=nfk_total_mm,
        )


    def get_irrigation_events(self):
        pass

    def calculate_evapotranspiration(self, meteo_data):
        pass

    def calculate_water_balance(self):
        pass