import { useEffect, useState } from "react";

import { getNetworkSummary } from "../api/client";

import LoadingState from "../components/LoadingState";
import ErrorState from "../components/ErrorState";


function MetricCard({ label, value }) {
  return (
    <article className="kpi-card">
      <span className="kpi-label">
        {label}
      </span>

      <strong className="kpi-value">
        {value}
      </strong>
    </article>
  );
}


function Dashboard() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);


  useEffect(() => {
    let mounted = true;

    async function loadSummary() {
      try {
        setLoading(true);
        setError(null);

        const data = await getNetworkSummary();

        if (mounted) {
          setSummary(data);
        }
      } catch (err) {
        if (mounted) {
          setError(
            err instanceof Error
              ? err.message
              : "Unable to retrieve network summary."
          );
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }

    loadSummary();

    return () => {
      mounted = false;
    };
  }, []);


  if (loading) {
    return <LoadingState />;
  }


  if (error) {
    return (
      <main className="dashboard">
        <div className="api-status-banner">
          API unavailable
        </div>

        <ErrorState
          message={error}
        />
      </main>
    );
  }


  if (!summary) {
    return (
      <main className="dashboard">
        <div className="api-status-banner">
          No network summary data available.
        </div>
      </main>
    );
  }


  return (
    <main className="dashboard">

      {/* =================================================
          Page Header
          ================================================= */}

      <header className="dashboard-header">

        <div>
          <p className="eyebrow">
            NETWORK OPERATIONS CENTER
          </p>

          <h1>
            Network Overview
          </h1>

          <p>
            Current network activity summary
          </p>
        </div>


        {/* ===============================================
            API AS_OF
            =============================================== */}

        <div className="as-of">

          <span>
            REPORTING TIMESTAMP
          </span>

          <strong>
            {summary.as_of}
          </strong>

        </div>

      </header>


      {/* =================================================
          KPI Cards
          ================================================= */}

      <section
        className="kpi-grid"
        aria-label="Network KPIs"
      >

        <MetricCard
          label="Total Activity"
          value={Number(
            summary.total_activity
          ).toLocaleString()}
        />


        <MetricCard
          label="Active Grids"
          value={Number(
            summary.active_grids
          ).toLocaleString()}
        />


        <MetricCard
          label="Peak Hour"
          value={summary.peak_hour}
        />


        <MetricCard
          label="Top Grid"
          value={`Grid ${summary.top_grid}`}
        />

      </section>

    </main>
  );
}


export default Dashboard;