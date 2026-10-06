// =========================================================
// Phase 5 — Application Router
// File: phase5/src/App.jsx
// =========================================================

import {
  BrowserRouter,
  Routes,
  Route,
} from "react-router-dom";

import Navbar from "./components/Navbar";

import Dashboard from "./pages/Dashboard";
import GridExplorer from "./pages/GridExplorer";
import HotspotsAlerts from "./pages/HotspotsAlerts";
import PredictiveRisk from "./pages/PredictiveRisk";


// =========================================================
// App
// =========================================================

function App() {

  return (

    <BrowserRouter>

      <div className="app">

        <Navbar />

        <Routes>

          {/* RE2 */}

          <Route
            path="/"
            element={<Dashboard />}
          />


          {/* RE3 */}

          <Route
            path="/grid"
            element={<GridExplorer />}
          />

          <Route
            path="/grid/:gridId"
            element={<GridExplorer />}
          />


          {/* RE4 */}

          <Route
            path="/hotspots"
            element={<HotspotsAlerts />}
          />


          {/* RE5 */}

          <Route
            path="/predict-risk"
            element={<PredictiveRisk />}
          />


          {/* Fallback */}

          <Route
            path="*"
            element={<Dashboard />}
          />

        </Routes>

      </div>

    </BrowserRouter>
  );
}


export default App;