// =========================================================
// RE3 — Grid Explorer
// File: phase5/src/pages/GridExplorer.jsx
// =========================================================

import {
  useEffect,
  useState,
} from "react";

import {
  useParams,
} from "react-router-dom";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

import {
  getGridActivity,
} from "../api/client";


// =========================================================
// Grid Explorer
// =========================================================

function GridExplorer() {

  const {
    gridId: urlGridId,
  } = useParams();


  const [
    gridId,
    setGridId
  ] = useState(
    urlGridId ?? ""
  );


  const [
    activity,
    setActivity
  ] = useState([]);


  const [
    asOf,
    setAsOf
  ] = useState(null);


  const [
    loading,
    setLoading
  ] = useState(false);


  const [
    error,
    setError
  ] = useState(null);


  const [
    searched,
    setSearched
  ] = useState(false);


  // =======================================================
  // Load grid from URL
  // =======================================================

  useEffect(() => {

    if (!urlGridId) {
      return;
    }


    setGridId(urlGridId);

    loadGridActivity(
      urlGridId
    );

  }, [urlGridId]);


  // =======================================================
  // API request
  // =======================================================

  async function loadGridActivity(
    value
  ) {

    setError(null);
    setActivity([]);
    setAsOf(null);
    setSearched(true);


    const parsedGridId =
      Number(value);


    // -----------------------------------------------------
    // Validate grid ID
    // -----------------------------------------------------

    if (
      !Number.isInteger(parsedGridId) ||
      parsedGridId < 1 ||
      parsedGridId > 10000
    ) {

      setError(
        "Enter a grid ID between 1 and 10000."
      );

      return;
    }


    // -----------------------------------------------------
    // API request
    // -----------------------------------------------------

    try {

      setLoading(true);


      const response =
        await getGridActivity(
          parsedGridId
        );


      // ---------------------------------------------------
      // API2 response:
      //
      // {
      //   grid_id: 4821,
      //   as_of: "...",
      //   activity: [...]
      // }
      // ---------------------------------------------------

      if (
        !response ||
        !Array.isArray(
          response.activity
        )
      ) {

        throw new Error(
          "Unexpected response format from the API."
        );
      }


      setActivity(
        response.activity
      );


      setAsOf(
        response.as_of ?? null
      );

    } catch (err) {

      setError(
        err instanceof Error
          ? err.message
          : "Unable to retrieve grid activity."
      );

    } finally {

      setLoading(false);
    }
  }


  // =======================================================
  // Form search
  // =======================================================

  function handleSearch(
    event
  ) {

    event.preventDefault();

    loadGridActivity(
      gridId
    );
  }


  // =======================================================
  // Prepare chart data
  // =======================================================

  const chartData =
    activity.map(
      (record) => ({

        time:
          new Date(
            record.timestamp
          ).toLocaleTimeString(
            [],
            {
              hour: "2-digit",
              minute: "2-digit",
              hour12: false,
            }
          ),

        sms:
          Number(
            record.total_sms
          ),

        calls:
          Number(
            record.total_calls
          ),

        internet:
          Number(
            record.internet_activity
          ),

        total:
          Number(
            record.total_activity
          ),

      })
    );


  // =======================================================
  // Render
  // =======================================================

  return (

    <main className="grid-explorer">

      {/* =================================================
          Header
          ================================================= */}

      <header className="page-header">

        <p className="eyebrow">
          NETWORK OPERATIONS CENTER
        </p>

        <h1>
          Grid Explorer
        </h1>

        <p>
          View recent hourly activity for a grid.
        </p>

      </header>


      {/* =================================================
          Search
          ================================================= */}

      <section className="grid-search">

        <form
          onSubmit={handleSearch}
          className="grid-search-form"
        >

          <label htmlFor="grid-id">
            Grid ID
          </label>


          <input
            id="grid-id"
            type="number"
            min="1"
            max="10000"
            value={gridId}
            onChange={(event) =>
              setGridId(
                event.target.value
              )
            }
            placeholder="Enter grid ID"
          />


          <button
            type="submit"
            disabled={loading}
          >
            {loading
              ? "Loading..."
              : "Search"}
          </button>

        </form>

      </section>


      {/* =================================================
          Error state
          ================================================= */}

      {error && (

        <section
          className="grid-error"
          role="alert"
        >

          <strong>
            Unable to load grid
          </strong>

          <p>
            {error}
          </p>

        </section>

      )}


      {/* =================================================
          Empty state
          ================================================= */}

      {!loading &&
        searched &&
        !error &&
        activity.length === 0 && (

          <section className="grid-empty">

            <h2>
              No activity data
            </h2>

            <p>
              No activity records were returned
              for this grid.
            </p>

          </section>
        )}


      {/* =================================================
          Results
          ================================================= */}

      {!loading &&
        !error &&
        activity.length > 0 && (

          <section className="grid-results">

            {/* ===========================================
                Result header
                =========================================== */}

            <div className="grid-result-header">

              <div>

                <p className="eyebrow">
                  GRID
                </p>

                <h2>
                  Grid {gridId}
                </h2>

              </div>


              {asOf && (

                <div className="as-of">

                  <span>
                    REPORTING TIMESTAMP
                  </span>

                  <strong>
                    {asOf}
                  </strong>

                </div>

              )}

            </div>


            {/* ===========================================
                Record count
                =========================================== */}

            <div className="grid-summary">

              <span>
                {activity.length} hourly records
              </span>

            </div>


            {/* ===========================================
                Time-Series Chart
                =========================================== */}

            <section className="activity-chart">

              <div className="chart-header">

                <p className="eyebrow">
                  ACTIVITY TREND
                </p>

                <h2>
                  Hourly Network Activity
                </h2>

                <p>
                  Activity across the selected grid
                  over the reporting window.
                </p>

              </div>


              <div
                className="chart-container"
                style={{
                  width: "100%",
                  height: 400,
                }}
              >

                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >

                  <LineChart
                    data={chartData}
                    margin={{
                      top: 20,
                      right: 30,
                      left: 20,
                      bottom: 20,
                    }}
                  >

                    <CartesianGrid
                      strokeDasharray="3 3"
                    />


                    <XAxis
                      dataKey="time"
                      tick={{
                        fontSize: 12,
                      }}
                    />


                    <YAxis
                      tick={{
                        fontSize: 12,
                      }}
                    />


                    <Tooltip />

                    <Legend />


                    <Line
                      type="monotone"
                      dataKey="sms"
                      name="SMS Activity"
                      stroke="#2563eb"
                      strokeWidth={2}
                      dot={false}
                    />


                    <Line
                      type="monotone"
                      dataKey="calls"
                      name="Call Activity"
                      stroke="#16a34a"
                      strokeWidth={2}
                      dot={false}
                    />


                    <Line
                      type="monotone"
                      dataKey="internet"
                      name="Internet Activity"
                      stroke="#9333ea"
                      strokeWidth={2}
                      dot={false}
                    />


                    <Line
                      type="monotone"
                      dataKey="total"
                      name="Total Activity"
                      stroke="#dc2626"
                      strokeWidth={3}
                      dot={false}
                    />

                  </LineChart>

                </ResponsiveContainer>

              </div>

            </section>


            {/* ===========================================
                Activity table
                =========================================== */}

            <div className="table-container">

              <table className="activity-table">

                <thead>

                  <tr>

                    <th>
                      Timestamp
                    </th>

                    <th>
                      SMS Activity
                    </th>

                    <th>
                      Call Activity
                    </th>

                    <th>
                      Internet Activity
                    </th>

                    <th>
                      Total Activity
                    </th>

                  </tr>

                </thead>


                <tbody>

                  {activity.map(
                    (record) => (

                      <tr
                        key={
                          `${gridId}-${record.timestamp}`
                        }
                      >

                        <td>
                          {record.timestamp}
                        </td>


                        <td>
                          {Number(
                            record.total_sms
                          ).toLocaleString()}
                        </td>


                        <td>
                          {Number(
                            record.total_calls
                          ).toLocaleString()}
                        </td>


                        <td>
                          {Number(
                            record.internet_activity
                          ).toLocaleString()}
                        </td>


                        <td>

                          <strong>
                            {Number(
                              record.total_activity
                            ).toLocaleString()}
                          </strong>

                        </td>

                      </tr>

                    )
                  )}

                </tbody>

              </table>

            </div>

          </section>
        )}

    </main>
  );
}


export default GridExplorer;