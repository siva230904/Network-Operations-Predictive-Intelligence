// =========================================================
// RE4 — Hotspots, Alerts & Milan Map
// File: phase5/src/pages/HotspotsAlerts.jsx
// =========================================================

import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  useNavigate,
} from "react-router-dom";

import {
  getHotspots,
  getAlerts,
} from "../api/client";

import MilanGridMap from "../components/MilanGridMap";


// =========================================================
// Hotspots & Alerts
// =========================================================

function HotspotsAlerts() {

  const navigate =
    useNavigate();


  // =======================================================
  // State
  // =======================================================

  const [
    geoJson,
    setGeoJson
  ] = useState(null);

  const [
    hotspots,
    setHotspots
  ] = useState([]);

  const [
    alerts,
    setAlerts
  ] = useState([]);

  const [
    limit,
    setLimit
  ] = useState(10);

  const [
    severity,
    setSeverity
  ] = useState("");

  const [
    loading,
    setLoading
  ] = useState(true);

  const [
    error,
    setError
  ] = useState(null);

  const [
    geoJsonError,
    setGeoJsonError
  ] = useState(null);


  // =======================================================
  // Load GeoJSON ONCE
  // =======================================================

  useEffect(() => {

    let cancelled = false;

    async function loadGeoJson() {

      try {

        const response =
          await fetch(
            "/reference/milano-grid.geojson"
          );

        if (!response.ok) {

          throw new Error(
            `Unable to load Milan grid reference `
            + `(${response.status}).`
          );
        }

        const data =
          await response.json();

        if (!cancelled) {

          setGeoJson(data);
        }

      } catch (err) {

        if (!cancelled) {

          setGeoJsonError(
            err instanceof Error
              ? err.message
              : "Unable to load Milan grid reference."
          );
        }
      }
    }

    loadGeoJson();

    return () => {
      cancelled = true;
    };

  }, []);


  // =======================================================
  // Load API data
  // =======================================================

  useEffect(() => {

    let cancelled = false;

    async function loadOperationalData() {

      try {

        setLoading(true);
        setError(null);


        const [
          hotspotData,
          alertData
        ] = await Promise.all([

          getHotspots(
            limit
          ),

          getAlerts(
            limit,
            severity || null
          ),

        ]);


        if (cancelled) {
          return;
        }


        if (!Array.isArray(hotspotData)) {

          throw new Error(
            "Unexpected hotspot response format."
          );
        }


        if (!Array.isArray(alertData)) {

          throw new Error(
            "Unexpected alert response format."
          );
        }


        setHotspots(
          hotspotData
        );

        setAlerts(
          alertData
        );

      } catch (err) {

        if (!cancelled) {

          setError(
            err instanceof Error
              ? err.message
              : "Unable to load operational data."
          );
        }

      } finally {

        if (!cancelled) {

          setLoading(false);
        }
      }
    }


    loadOperationalData();


    return () => {
      cancelled = true;
    };

  }, [
    limit,
    severity,
  ]);


  // =======================================================
  // Create ranked operational rows
  // =======================================================

  const rankedRows = useMemo(() => {

    const rows = [];


    // -----------------------------------------------------
    // Add hotspots
    // -----------------------------------------------------

    for (
      const hotspot of hotspots
    ) {

      rows.push({

        grid_id:
          Number(
            hotspot.grid_id
          ),

        timestamp:
          hotspot.timestamp,

        activity:
          Number(
            hotspot.total_activity
          ),

        status:
          "ATTENTION",

        severity:
          null,

        type:
          "HOTSPOT",

        reason:
          "High activity area",

      });
    }


    // -----------------------------------------------------
    // Alerts override matching grid status
    // -----------------------------------------------------

    for (
      const alert of alerts
    ) {

      const severityValue =
        String(
          alert.severity || ""
        ).toUpperCase();

      const status =
        severityValue === "HIGH"
          ? "HIGH"
          : "ATTENTION";


      rows.push({

        grid_id:
          Number(
            alert.grid_id
          ),

        timestamp:
          alert.timestamp,

        activity:
          Number(
            alert.current_activity
          ),

        status,

        severity:
          severityValue,

        type:
          "ALERT",

        reason:
          alert.reason,

      });
    }


    // -----------------------------------------------------
    // Deterministic ordering
    // -----------------------------------------------------

    rows.sort(
      (a, b) => {

        const statusRank = {
          HIGH: 3,
          ATTENTION: 2,
          NORMAL: 1,
        };

        const statusDifference =
          statusRank[b.status]
          -
          statusRank[a.status];

        if (
          statusDifference !== 0
        ) {

          return statusDifference;
        }


        if (
          b.activity !==
          a.activity
        ) {

          return (
            b.activity
            -
            a.activity
          );
        }


        if (
          a.grid_id !==
          b.grid_id
        ) {

          return (
            a.grid_id
            -
            b.grid_id
          );
        }


        return String(
          a.timestamp
        ).localeCompare(
          String(b.timestamp)
        );
      }
    );


    return rows;

  }, [
    hotspots,
    alerts,
  ]);

  // =======================================================
  // Map data
  // =======================================================
  
  const mapHotspots =
    severity
      ? []
      : hotspots;
  // =======================================================
  // Render
  // =======================================================

  return (

    <main className="hotspots-alerts">

      {/* =================================================
          Header
          ================================================= */}

      <header className="page-header">

        <p className="eyebrow">
          NETWORK OPERATIONS CENTER
        </p>

        <h1>
          Hotspots & Alerts
        </h1>

        <p>
          Prioritized operational attention areas
          across the Milan grid.
        </p>

      </header>


      {/* =================================================
          Controls
          ================================================= */}

      <section className="re4-controls">

        <div>

          <label htmlFor="limit">
            Limit
          </label>

          <select
            id="limit"
            value={limit}
            onChange={(event) =>
              setLimit(
                Number(
                  event.target.value
                )
              )
            }
          >

            <option value="5">
              5
            </option>

            <option value="10">
              10
            </option>

            <option value="20">
              20
            </option>

            <option value="50">
              50
            </option>

            <option value="100">
              100
            </option>

          </select>

        </div>


        <div>

          <label htmlFor="severity">
            Severity
          </label>

          <select
            id="severity"
            value={severity}
            onChange={(event) =>
              setSeverity(
                event.target.value
              )
            }
          >

            <option value="">
              All
            </option>

            <option value="HIGH">
              HIGH
            </option>

            <option value="MEDIUM">
              MEDIUM
            </option>

            <option value="LOW">
              LOW
            </option>

          </select>

        </div>

      </section>


      {/* =================================================
          API error
          ================================================= */}

      {error && (

        <section
          className="grid-error"
          role="alert"
        >

          <strong>
            Unable to load operational data
          </strong>

          <p>
            {error}
          </p>

        </section>
      )}


      {/* =================================================
          GeoJSON error
          ================================================= */}

      {geoJsonError && (

        <section
          className="grid-error"
          role="alert"
        >

          <strong>
            Unable to load Milan grid
          </strong>

          <p>
            {geoJsonError}
          </p>

        </section>
      )}


      {/* =================================================
          Status legend
          ================================================= */}

      <section className="status-legend">

        <div className="legend-item">

          <span className="status-marker normal">
          </span>

          <span>
            NORMAL
          </span>

        </div>


        <div className="legend-item">

          <span className="status-marker attention">
          </span>

          <span>
            ATTENTION
          </span>

        </div>


        <div className="legend-item">

          <span className="status-marker high">
          </span>

          <span>
            HIGH
          </span>

        </div>

      </section>


      {/* =================================================
          Map
          ================================================= */}

      {!geoJsonError && (

        <section className="map-section">

          <div className="section-header">

            <p className="eyebrow">
              MILAN GRID
            </p>

            <h2>
              Operational Map
            </h2>

            <p>
              Highlighted cells represent grids
              returned by the current operational APIs.
            </p>

          </div>


          {geoJson ? (

            <MilanGridMap
              geoJson={geoJson}
              hotspots={mapHotspots}
              alerts={alerts}
            />

          ) : (

            <div className="map-loading">
              Loading Milan grid...
            </div>

          )}

        </section>
      )}


      {/* =================================================
          Ranked table
          ================================================= */}

      <section className="ranked-section">

        <div className="section-header">

          <p className="eyebrow">
            PRIORITIZED RESULTS
          </p>

          <h2>
            Operational Attention
          </h2>

        </div>


        {loading ? (

          <div className="map-loading">
            Loading operational data...
          </div>

        ) : rankedRows.length === 0 ? (

          <div className="grid-empty">

            <h2>
              No operational records
            </h2>

            <p>
              No hotspots or alerts matched
              the selected filters.
            </p>

          </div>

        ) : (

          <div className="table-container">

            <table className="activity-table">

              <thead>

                <tr>

                  <th>
                    Grid
                  </th>

                  <th>
                    Status
                  </th>

                  <th>
                    Type
                  </th>

                  <th>
                    Severity
                  </th>

                  <th>
                    Activity
                  </th>

                  <th>
                    Hour
                  </th>

                  <th>
                    Reason
                  </th>

                </tr>

              </thead>


              <tbody>

                {rankedRows.map(
                  (row, index) => (

                    <tr
                      key={
                        `${row.type}-`
                        + `${row.grid_id}-`
                        + `${row.timestamp}-`
                        + `${index}`
                      }
                      className={
                        `status-row status-${row.status.toLowerCase()}`
                      }
                    >

                      <td>

                        <button
                          type="button"
                          className="grid-link"
                          onClick={() =>
                            navigate(
                              `/grid/${row.grid_id}`
                            )
                          }
                        >
                          Grid {row.grid_id}
                        </button>

                      </td>


                      <td>

                        <span
                          className={
                            `status-badge status-${row.status.toLowerCase()}`
                          }
                        >

                          {row.status}

                        </span>

                      </td>


                      <td>
                        {row.type}
                      </td>


                      <td>
                        {row.severity || "—"}
                      </td>


                      <td>
                        {row.activity.toLocaleString()}
                      </td>


                      <td>
                        {row.timestamp}
                      </td>


                      <td>
                        {row.reason}
                      </td>

                    </tr>

                  )
                )}

              </tbody>

            </table>

          </div>
        )}

      </section>

    </main>
  );
}


export default HotspotsAlerts;

