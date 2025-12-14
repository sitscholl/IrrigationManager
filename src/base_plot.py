# plotting/base_plot.py
from typing import Iterable, Optional, Sequence, Union
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

Number = Union[int, float]

class BasePlot:
    """
    A small wrapper around Plotly subplots for ET/soil-water dashboards.
    - create_base: builds the main layout (1 main + N subpanels)
    - plot_line: add a line to any row (panel)
    - plot_irrigation_events: add irrigation bars to a subpanel
    - render_streamlit: convenience helper for Streamlit pages
    Styling leans toward compact meteorological charts (framed axes, light grid, muted backgrounds).
    """

    def __init__(
        self,
        template: str = "plotly_white",
        colorway: Optional[Sequence[str]] = None,
    ):
        self.fig: Optional[go.Figure] = None
        self.template = template
        # Subtle, meteo-style palette
        self.colorway = list(colorway) if colorway else [
            "#d0b556",  # warm yellow
            "#c17846",  # muted orange/brown
            "#4cb281",  # mid green
            "#d94141",  # red
            "#6aa5c3",  # blue/teal
            "#7f8c8d",  # mid grey
            "#1f4e78",  # deep blue (max wind)
            "#b3b3b3",  # light grey accents
        ]
        # Keep track of colors actually used so marker overlays can match line colors
        self._trace_colors: dict[str, str] = {}

    # -------------------------------
    # Layout / panels
    # -------------------------------
    def create_base(
        self,
        subpanels: int = 1,
        row_heights: Optional[Sequence[float]] = None,
        shared_x: bool = True,
        vertical_spacing: float = 0.02,
        main_title: Optional[str] = None,
        show_legend: bool = True,
    ) -> "BasePlot":
        """
        Create a figure with 1 main row (top) + `subpanels` additional rows.

        row_heights: Optional custom ratios that sum to 1.
                     Defaults to [0.75] + equally split remainder for subs.
        """
        rows = 1 + max(0, subpanels)

        if row_heights is None:
            if subpanels <= 0:
                row_heights = [1.0]
            else:
                main = 0.75
                rest = (1.0 - main) / subpanels
                row_heights = [main] + [rest] * subpanels
        else:
            assert abs(sum(row_heights) - 1.0) < 1e-6, "row_heights must sum to 1"

        self.fig = make_subplots(
            rows=rows,
            cols=1,
            shared_xaxes=shared_x,
            row_heights=list(row_heights),
            vertical_spacing=vertical_spacing,
        )

        self.fig.update_layout(
            template=self.template,
            hovermode="x unified",   # one tooltip following the same x across panels
            title=({"text": main_title} if main_title else None),
            legend=dict(
                orientation="h",
                yanchor="top",
                y=1.1,
                xanchor="center",
                x=.5,
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="#d9d9d9",
                borderwidth=1,
                itemwidth=70,
                font=dict(size=12, color="#2f2f2f"),
            ) if show_legend else dict(),
            margin=dict(l=70, r=30, t=50 if main_title else 25, b=40),
            plot_bgcolor="#f9f9f9",
            paper_bgcolor="#ffffff",
            font=dict(family="Arial, Helvetica, sans-serif", size=12, color="#2f2f2f"),
            hoverlabel=dict(
                bgcolor="#ffffff",
                bordercolor="#bfbfbf",
                font=dict(size=12, color="#2f2f2f"),
            ),
            colorway=self.colorway,
        )
        # date axis quality-of-life
        self.fig.update_xaxes(
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikethickness=1,
            showgrid=True,
            gridcolor="#dcdcdc",
            gridwidth=1,
            zeroline=False,
            showline=True,
            linewidth=1.4,
            linecolor="#2f2f2f",
            mirror=True,
            ticks="outside",
            ticklen=6,
            tickcolor="#2f2f2f",
            tickfont=dict(size=11),
            tickformatstops=[
                dict(dtickrange=[None, 1000 * 60 * 60 * 24], value="%H:%M"),
                dict(dtickrange=[1000 * 60 * 60 * 24, None], value="%d.%m\n%H:%M"),
            ],
        )
        self.fig.update_yaxes(
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikethickness=1,
            showgrid=True,
            gridcolor="#e3e3e3",
            gridwidth=1,
            zeroline=False,
            showline=True,
            linewidth=1.4,
            linecolor="#2f2f2f",
            mirror=True,
            ticks="outside",
            ticklen=6,
            tickcolor="#2f2f2f",
            tickfont=dict(size=11),
        )
        return self

    # -------------------------------
    # Lines
    # -------------------------------
    def plot_line(
        self,
        x: Union[pd.Series, Iterable],
        y: Union[pd.Series, Iterable],
        *,
        row: int = 1,
        name: Optional[str] = None,
        width: int = 2,
        dash: Optional[str] = None,   # "dot", "dash", "dashdot"
        markers: bool = False,
        hover_name: Optional[str] = None,
        hover_units: Optional[str] = None,
        color: Optional[str] = None,
        legendgroup: Optional[str] = None,
    ) -> "BasePlot":
        """
        Add a line (optionally with markers) to a given panel row (1-indexed).
        """
        assert self.fig is not None, "Call create_base() first."

        mode = "lines+markers" if markers else "lines"
        hovertemplate = "%{x}<br>%{y}"
        if hover_units:
            hovertemplate = f"%{{x}}<br>%{{y}} {hover_units}"
        if hover_name:
            hovertemplate = f"{hover_name}<br>" + hovertemplate

        # normalize x to plain datetimes so JSON serialization works
        x_vals = pd.to_datetime(x).to_pydatetime().tolist()
        # mimic Plotly's colorway assignment so we can reuse the color for overlays
        trace_color = color or self.colorway[len(self.fig.data) % len(self.colorway)]
        legendgroup = legendgroup or name
        self.fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=list(y),
                name=name,
                mode=mode,
                legendgroup=legendgroup,
                line=dict(color=trace_color, width=width, dash=dash) if dash else dict(color=trace_color, width=width),
                hovertemplate=hovertemplate,
            ),
            row=row,
            col=1,
        )
        if name:
            self._trace_colors[name] = trace_color
        return self

    def plot_event_markers(
        self,
        x: Union[pd.Series, Iterable],
        y: Union[pd.Series, Iterable],
        *,
        mask: Union[pd.Series, Iterable],
        row: int = 1,
        name: Optional[str] = None,
        symbol: str = "circle",
        size: int = 8,
        hover_name: Optional[str] = None,
        hover_units: Optional[str] = None,
        color: Optional[str] = None,
        legendgroup: Optional[str] = None,
        opacity: float = 0.95,
        show_in_legend: bool = False,
    ) -> "BasePlot":
        """
        Plot markers on an existing line for the positions where `mask` is truthy.
        This is useful for highlighting precipitation/irrigation days on the main panel.
        """
        assert self.fig is not None, "Call create_base() first."

        x_series = pd.Series(pd.to_datetime(x))
        y_series = pd.Series(y).reset_index(drop=True)
        mask_series = pd.Series(mask).fillna(False).astype(bool).reset_index(drop=True)

        if len(x_series) != len(y_series) or len(x_series) != len(mask_series):
            raise ValueError("x, y, and mask must have the same length for event markers.")

        event_mask = mask_series
        if not event_mask.any():
            return self

        x_markers = x_series[event_mask].dt.to_pydatetime().tolist()
        y_markers = y_series[event_mask].tolist()
        legendgroup = legendgroup or name
        # Prefer the color of the corresponding line if known
        trace_color = color or (name and self._trace_colors.get(name)) or self.colorway[len(self.fig.data) % len(self.colorway)]

        # hovertemplate = "%{x}<br>%{y}"
        # if hover_units:
        #     hovertemplate = f"%{{x}}<br>%{{y}} {hover_units}"
        # if hover_name:
        #     hovertemplate = f"{hover_name}<br>" + hovertemplate

        self.fig.add_trace(
            go.Scatter(
                x=x_markers,
                y=y_markers,
                name=name,
                mode="markers",
                legendgroup=legendgroup,
                marker=dict(
                    symbol=symbol,
                    size=size,
                    color=trace_color,
                    line=dict(color="#ffffff", width=1),
                    opacity=opacity,
                ),
                hoverinfo="none",
                showlegend=show_in_legend and bool(name),
            ),
            row=row,
            col=1,
        )
        return self

    # -------------------------------
    # Irrigation events
    # -------------------------------
    def plot_irrigation_events(
        self,
        times: Sequence[pd.Timestamp],
        *,
        row: int = 2,
        bar_width: Optional[pd.Timedelta] = None,
        name: str = "Irrigation (mm)",
        opacity: float = 0.5,
    ) -> "BasePlot":
        """
        Plot irrigation as vertical bars in a subpanel (default: row 2).
        times: datetimes of the event starts
        bar_width: width of the bars; if None, try to infer from median spacing
        """
        assert self.fig is not None, "Call create_base() first."
        if not len(times):
            return self

        # Infer a sensible width if not provided
        if bar_width is None and len(times) > 1:
            s = pd.Series(pd.to_datetime(times)).sort_values().diff().median()
            if pd.isna(s) or s is pd.NaT:
                s = pd.Timedelta(hours=6)
            bar_width = s * 0.8
        if bar_width is None:
            bar_width = pd.Timedelta(hours=6)

        centers = pd.to_datetime(times)
        lefts = centers - bar_width / 2
        rights = centers + bar_width / 2

        # Plot as a histogram-like bar trace
        centers = pd.to_datetime(times).to_pydatetime().tolist()
        self.fig.add_trace(
            go.Bar(
                x=centers,
                y=[1]*len(centers),
                name=name,
                opacity=opacity,
                marker=dict(
                    color="#5c9be6",
                    line=dict(width=0),
                ),
                width=[(r - l).total_seconds() * 1000 for l, r in zip(lefts, rights)],  # ms
                hovertemplate="%{x}<br>%{y} mm",
            ),
            row=row,
            col=1,
        )
        # Keep the irrigation panel clean and focused on the bars
        self.fig.update_yaxes(
            title_text=name,
            showgrid=False,
            zeroline=False,
            rangemode="tozero",
            row=row,
            col=1,
        )
        return self

    # -------------------------------
    # Misc helpers
    # -------------------------------
    def set_yaxis_title(self, title: str, *, row: int = 1) -> "BasePlot":
        assert self.fig is not None, "Call create_base() first."
        self.fig.update_yaxes(title_text=title, row=row, col=1)
        return self

    def render_streamlit(self, st, *, use_container_width: bool = True, height: int = 600):
        """Convenience wrapper for Streamlit."""
        assert self.fig is not None, "Call create_base() first."
        self.fig.update_layout(height=height)
        st.plotly_chart(self.fig, use_container_width=use_container_width)

if __name__ == '__main__':
    # app.py
    import streamlit as st
    import pandas as pd
    import numpy as np

    # st.set_page_config(page_title="ET & Water Balance", layout="wide")

    # Fake data
    idx = pd.date_range("2025-05-01", periods=24*3, freq="H")
    air_T = 10 + 5*np.sin(np.linspace(0, 6*np.pi, len(idx)))
    soil_T = 8 + 3*np.sin(np.linspace(0, 6*np.pi, len(idx)) - 0.7)
    et_mm = np.clip(0.08 + 0.05*np.sin(np.linspace(0, 6*np.pi, len(idx))), 0, None)

    irrig_times = pd.to_datetime(["2025-05-01 06:00", "2025-05-02 19:00"])
    irrig_mm = [6, 8]

    bp = BasePlot().create_base(subpanels=1, vertical_spacing=0.1, main_title="Microclimate & Irrigation")
    bp.set_yaxis_title("Temperature / ET", row=1)
    bp.plot_line(idx, air_T, name="Air Temp (°C)")
    bp.plot_line(idx, soil_T, name="Soil Temp 25cm (°C)", dash="dash")
    bp.plot_line(idx, et_mm, name="ET (mm)", markers=True)
    bp.plot_irrigation_events(irrig_times, irrig_mm, row=2, name="Irrigation (mm)")
    bp.fig.write_html("debug_plot.html")
    # bp.fig.show()

    print('App finished')
    # bp.render_streamlit(st, height=700)
