// =========================================================
// RE5 — Predictive Risk View
// File: phase5/src/pages/PredictiveRisk.jsx
// =========================================================

import {
  useState,
} from "react";

import {
  predictRisk,
} from "../api/client";


// =========================================================
// Initial form values
// =========================================================

const INITIAL_FORM = {
  grid_id: "",
  feature_timestamp: "",
};


// =========================================================
// Predictive Risk
// =========================================================

function PredictiveRisk() {

  const [
    form,
    setForm
  ] = useState(
    INITIAL_FORM
  );


  const [
    result,
    setResult
  ] = useState(null);


  const [
    loading,
    setLoading
  ] = useState(false);


  const [
    error,
    setError
  ] = useState(null);


  // =======================================================
  // Input change
  // =======================================================

  function handleChange(
    event
  ) {

    const {
      name,
      value,
    } = event.target;


    setForm(
      (previous) => ({
        ...previous,
        [name]: value,
      })
    );
  }


  // =======================================================
  // Submit prediction
  // =======================================================

  async function handleSubmit(
    event
  ) {

    event.preventDefault();


    setError(null);
    setResult(null);


    // -----------------------------------------------------
    // Validate Grid ID
    // -----------------------------------------------------

    const gridId =
      Number(
        form.grid_id
      );


    if (
      !Number.isInteger(
        gridId
      ) ||
      gridId < 1 ||
      gridId > 10000
    ) {

      setError(
        "Grid ID must be between 1 and 10000."
      );

      return;
    }


    // -----------------------------------------------------
    // Validate timestamp
    // -----------------------------------------------------

    if (
      !form.feature_timestamp
    ) {

      setError(
        "Feature timestamp is required."
      );

      return;
    }


    // -----------------------------------------------------
    // Build API payload
    // -----------------------------------------------------

    const payload = {

      grid_id:
        gridId,

      feature_timestamp:
        form.feature_timestamp,

    };


    // -----------------------------------------------------
    // API request
    // -----------------------------------------------------

    try {

      setLoading(true);


      const prediction =
        await predictRisk(
          payload
        );


      setResult(
        prediction
      );

    } catch (err) {

      setError(
        err instanceof Error
          ? err.message
          : "Unable to generate risk prediction."
      );

    } finally {

      setLoading(false);
    }
  }


  // =======================================================
  // Render
  // =======================================================

  return (

    <main className="predictive-risk">

      {/* =================================================
          Header
          ================================================= */}

      <header className="page-header">

        <p className="eyebrow">
          NETWORK OPERATIONS CENTER
        </p>

        <h1>
          Predictive Risk
        </h1>

        <p>
          Select a grid and feature timestamp to
          request a next-hour network risk prediction.
        </p>

      </header>


      {/* =================================================
          Prediction Input
          ================================================= */}

      <section className="prediction-input-section">

        <div className="section-header">

          <p className="eyebrow">
            PREDICTION INPUT
          </p>

          <h2>
            Grid and Timestamp
          </h2>

          <p>
            Provide the grid and timestamp. The prediction
            service retrieves the required ML2 features
            automatically.
          </p>

        </div>


        <form
          className="prediction-form"
          onSubmit={handleSubmit}
        >

          {/* =============================================
              Grid ID
              ============================================= */}

          <div className="prediction-field">

            <label htmlFor="prediction-grid-id">
              Grid ID
            </label>

            <input
              id="prediction-grid-id"
              name="grid_id"
              type="number"
              min="1"
              max="10000"
              step="1"
              value={form.grid_id}
              onChange={handleChange}
              placeholder="1–10000"
              required
            />

          </div>


          {/* =============================================
              Feature Timestamp
              ============================================= */}

          <div className="prediction-field">

            <label htmlFor="feature-timestamp">
              Feature Timestamp
            </label>

            <input
              id="feature-timestamp"
              name="feature_timestamp"
              type="datetime-local"
              value={form.feature_timestamp}
              onChange={handleChange}
              required
            />

          </div>


          {/* =============================================
              Submit
              ============================================= */}

          <div className="prediction-submit">

            <button
              type="submit"
              disabled={loading}
            >
              {loading
                ? "Generating Prediction..."
                : "Predict Risk"}
            </button>

          </div>

        </form>

      </section>


      {/* =================================================
          Error
          ================================================= */}

      {error && (

        <section
          className="grid-error"
          role="alert"
        >

          <strong>
            Prediction unavailable
          </strong>

          <p>
            {error}
          </p>

        </section>

      )}


      {/* =================================================
          Model Output
          ================================================= */}

      {result && (

        <section className="model-output-section">

          <div className="section-header">

            <p className="eyebrow">
              MODEL OUTPUT
            </p>

            <h2>
              Predictive Risk Result
            </h2>

            <p>
              The prediction uses the ML2 features
              stored for the requested grid and timestamp.
            </p>

          </div>


          <div className="model-output-card">

            {/* =========================================
                Risk Score
                ========================================= */}

            <div className="model-output-item">

              <span className="model-output-label">
                Risk Score
              </span>

              <strong className="model-output-score">
                {Number(
                  result.risk_score
                ).toFixed(3)}
              </strong>

              <span className="model-output-note">
                Model score between 0 and 1
              </span>

            </div>


            {/* =========================================
                Risk Level
                ========================================= */}

            <div className="model-output-item">

              <span className="model-output-label">
                Risk Level
              </span>

              <strong className="model-output-level">
                {result.risk_level}
              </strong>

            </div>


            {/* =========================================
                Model Version
                ========================================= */}

            <div className="model-output-item">

              <span className="model-output-label">
                Model Version
              </span>

              <strong className="model-output-version">
                {result.model_version}
              </strong>

            </div>


            {/* =========================================
                Feature Timestamp
                ========================================= */}

            <div className="model-output-item">

              <span className="model-output-label">
                Feature Timestamp
              </span>

              <strong className="model-output-version">
                {result.feature_timestamp}
              </strong>

            </div>

          </div>

        </section>

      )}


      {/* =================================================
          Explanation — Separate from Model Output
          ================================================= */}

      {result && (

        <section className="prediction-explanation-section">

          <div className="section-header">

            <p className="eyebrow">
              EXPLANATION
            </p>

            <h2>
              Model Interpretation
            </h2>

            <p>
              Narrative explanation is kept separate
              from the model output.
            </p>

          </div>


          <div className="explanation-card">

            <div className="explanation-placeholder">

              <h3>
                Explain with AI
              </h3>

              <p>
                AI explanation will be available
                in the later Network Operations
                Assistant phase.
              </p>

              <button
                type="button"
                disabled
              >
                Explain with AI
              </button>

            </div>

          </div>

        </section>

      )}

    </main>
  );
}


export default PredictiveRisk;

// // =========================================================
// // RE5 — Predictive Risk View
// // File: phase5/src/pages/PredictiveRisk.jsx
// // =========================================================

// import {
//   useState,
// } from "react";

// import {
//   predictRisk,
// } from "../api/client";


// // =========================================================
// // Initial form values
// // =========================================================

// const INITIAL_FORM = {
//   grid_id: "",
//   avg_activity: "",
//   activity_growth: "",
//   active_hours: "",
//   peak_ratio: "",
//   variability: "",
//   internet_share: "",
// };


// // =========================================================
// // Predictive Risk
// // =========================================================

// function PredictiveRisk() {

//   const [
//     form,
//     setForm
//   ] = useState(
//     INITIAL_FORM
//   );


//   const [
//     result,
//     setResult
//   ] = useState(null);


//   const [
//     loading,
//     setLoading
//   ] = useState(false);


//   const [
//     error,
//     setError
//   ] = useState(null);


//   // =======================================================
//   // Input change
//   // =======================================================

//   function handleChange(
//     event
//   ) {

//     const {
//       name,
//       value,
//     } = event.target;


//     setForm(
//       (previous) => ({
//         ...previous,
//         [name]: value,
//       })
//     );
//   }


//   // =======================================================
//   // Submit prediction
//   // =======================================================

//   async function handleSubmit(
//     event
//   ) {

//     event.preventDefault();


//     setError(null);
//     setResult(null);


//     // -----------------------------------------------------
//     // Convert form values to correct API types
//     // -----------------------------------------------------

//     const payload = {

//       grid_id:
//         Number(
//           form.grid_id
//         ),

//       avg_activity:
//         Number(
//           form.avg_activity
//         ),

//       activity_growth:
//         Number(
//           form.activity_growth
//         ),

//       active_hours:
//         Number(
//           form.active_hours
//         ),

//       peak_ratio:
//         Number(
//           form.peak_ratio
//         ),

//       variability:
//         Number(
//           form.variability
//         ),

//       internet_share:
//         Number(
//           form.internet_share
//         ),

//     };


//     // -----------------------------------------------------
//     // Client-side validation
//     // -----------------------------------------------------

//     if (
//       !Number.isInteger(
//         payload.grid_id
//       ) ||
//       payload.grid_id < 1 ||
//       payload.grid_id > 10000
//     ) {

//       setError(
//         "Grid ID must be between 1 and 10000."
//       );

//       return;
//     }


//     if (
//       payload.avg_activity < 0
//     ) {

//       setError(
//         "Average activity must be 0 or greater."
//       );

//       return;
//     }


//     if (
//       payload.active_hours < 0
//     ) {

//       setError(
//         "Active hours must be 0 or greater."
//       );

//       return;
//     }


//     if (
//       payload.peak_ratio < 0
//     ) {

//       setError(
//         "Peak ratio must be 0 or greater."
//       );

//       return;
//     }


//     if (
//       payload.variability < 0
//     ) {

//       setError(
//         "Variability must be 0 or greater."
//       );

//       return;
//     }


//     if (
//       payload.internet_share < 0 ||
//       payload.internet_share > 1
//     ) {

//       setError(
//         "Internet share must be between 0 and 1."
//       );

//       return;
//     }


//     // -----------------------------------------------------
//     // API request
//     // -----------------------------------------------------

//     try {

//       setLoading(true);


//       const prediction =
//         await predictRisk(
//           payload
//         );


//       setResult(
//         prediction
//       );

//     } catch (err) {

//       setError(
//         err instanceof Error
//           ? err.message
//           : "Unable to generate risk prediction."
//       );

//     } finally {

//       setLoading(false);
//     }
//   }


//   // =======================================================
//   // Render
//   // =======================================================

//   return (

//     <main className="predictive-risk">

//       {/* =================================================
//           Header
//           ================================================= */}

//       <header className="page-header">

//         <p className="eyebrow">
//           NETWORK OPERATIONS CENTER
//         </p>

//         <h1>
//           Predictive Risk
//         </h1>

//         <p>
//           Submit a feature vector to request a
//           network risk prediction.
//         </p>

//       </header>


//       {/* =================================================
//           Feature Input
//           ================================================= */}

//       <section className="prediction-input-section">

//         <div className="section-header">

//           <p className="eyebrow">
//             PREDICTION INPUT
//           </p>

//           <h2>
//             Feature Values
//           </h2>

//           <p>
//             Provide the feature values required by
//             the prediction service.
//           </p>

//         </div>


//         <form
//           className="prediction-form"
//           onSubmit={handleSubmit}
//         >

//           {/* =============================================
//               Grid ID
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="prediction-grid-id">
//               Grid ID
//             </label>

//             <input
//               id="prediction-grid-id"
//               name="grid_id"
//               type="number"
//               min="1"
//               max="10000"
//               step="1"
//               value={form.grid_id}
//               onChange={handleChange}
//               placeholder="1–10000"
//               required
//             />

//           </div>


//           {/* =============================================
//               Average Activity
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="avg-activity">
//               Average Activity
//             </label>

//             <input
//               id="avg-activity"
//               name="avg_activity"
//               type="number"
//               min="0"
//               step="any"
//               value={form.avg_activity}
//               onChange={handleChange}
//               placeholder="e.g. 250"
//               required
//             />

//           </div>


//           {/* =============================================
//               Activity Growth
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="activity-growth">
//               Activity Growth
//             </label>

//             <input
//               id="activity-growth"
//               name="activity_growth"
//               type="number"
//               step="any"
//               value={form.activity_growth}
//               onChange={handleChange}
//               placeholder="e.g. 0.15"
//               required
//             />

//           </div>


//           {/* =============================================
//               Active Hours
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="active-hours">
//               Active Hours
//             </label>

//             <input
//               id="active-hours"
//               name="active_hours"
//               type="number"
//               min="0"
//               step="1"
//               value={form.active_hours}
//               onChange={handleChange}
//               placeholder="e.g. 18"
//               required
//             />

//           </div>


//           {/* =============================================
//               Peak Ratio
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="peak-ratio">
//               Peak Ratio
//             </label>

//             <input
//               id="peak-ratio"
//               name="peak_ratio"
//               type="number"
//               min="0"
//               step="any"
//               value={form.peak_ratio}
//               onChange={handleChange}
//               placeholder="e.g. 1.8"
//               required
//             />

//           </div>


//           {/* =============================================
//               Variability
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="variability">
//               Variability
//             </label>

//             <input
//               id="variability"
//               name="variability"
//               type="number"
//               min="0"
//               step="any"
//               value={form.variability}
//               onChange={handleChange}
//               placeholder="e.g. 0.25"
//               required
//             />

//           </div>


//           {/* =============================================
//               Internet Share
//               ============================================= */}

//           <div className="prediction-field">

//             <label htmlFor="internet-share">
//               Internet Share
//             </label>

//             <input
//               id="internet-share"
//               name="internet_share"
//               type="number"
//               min="0"
//               max="1"
//               step="any"
//               value={form.internet_share}
//               onChange={handleChange}
//               placeholder="0–1"
//               required
//             />

//           </div>


//           {/* =============================================
//               Submit
//               ============================================= */}

//           <div className="prediction-submit">

//             <button
//               type="submit"
//               disabled={loading}
//             >
//               {loading
//                 ? "Generating Prediction..."
//                 : "Predict Risk"}
//             </button>

//           </div>

//         </form>

//       </section>


//       {/* =================================================
//           Error
//           ================================================= */}

//       {error && (

//         <section
//           className="grid-error"
//           role="alert"
//         >

//           <strong>
//             Prediction unavailable
//           </strong>

//           <p>
//             {error}
//           </p>

//         </section>

//       )}


//       {/* =================================================
//           Model Output
//           ================================================= */}

//       {result && (

//         <section className="model-output-section">

//           <div className="section-header">

//             <p className="eyebrow">
//               MODEL OUTPUT
//             </p>

//             <h2>
//               Predictive Risk Result
//             </h2>

//             <p>
//               These values are returned directly
//               by the prediction service.
//             </p>

//           </div>


//           <div className="model-output-card">

//             {/* =========================================
//                 Risk Score
//                 ========================================= */}

//             <div className="model-output-item">

//               <span className="model-output-label">
//                 Risk Score
//               </span>

//               <strong className="model-output-score">
//                 {Number(
//                   result.risk_score
//                 ).toFixed(3)}
//               </strong>

//               <span className="model-output-note">
//                 Model score between 0 and 1
//               </span>

//             </div>


//             {/* =========================================
//                 Risk Level
//                 ========================================= */}

//             <div className="model-output-item">

//               <span className="model-output-label">
//                 Risk Level
//               </span>

//               <strong className="model-output-level">
//                 {result.risk_level}
//               </strong>

//             </div>


//             {/* =========================================
//                 Model Version
//                 ========================================= */}

//             <div className="model-output-item">

//               <span className="model-output-label">
//                 Model Version
//               </span>

//               <strong className="model-output-version">
//                 {result.model_version}
//               </strong>

//             </div>

//           </div>

//         </section>

//       )}


//       {/* =================================================
//           Explanation — Separate from Model Output
//           ================================================= */}

//       {result && (

//         <section className="prediction-explanation-section">

//           <div className="section-header">

//             <p className="eyebrow">
//               EXPLANATION
//             </p>

//             <h2>
//               Model Interpretation
//             </h2>

//             <p>
//               Narrative explanation is kept separate
//               from the model output.
//             </p>

//           </div>


//           <div className="explanation-card">

//             <div className="explanation-placeholder">

//               <h3>
//                 Explain with AI
//               </h3>

//               <p>
//                 AI explanation will be available
//                 in the later Network Operations
//                 Assistant phase.
//               </p>

//               <button
//                 type="button"
//                 disabled
//               >
//                 Explain with AI
//               </button>

//             </div>

//           </div>

//         </section>

//       )}

//     </main>
//   );
// }


// export default PredictiveRisk;