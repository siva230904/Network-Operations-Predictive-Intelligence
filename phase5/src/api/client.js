// // =========================================================
// // Phase 5 — API Client
// // File: phase5/src/api/client.js
// // =========================================================


// const API_BASE_URL =
//   import.meta.env.VITE_API_BASE_URL;


// // =========================================================
// // Environment validation
// // =========================================================

// if (!API_BASE_URL) {

//   throw new Error(
//     "VITE_API_BASE_URL is not configured."
//   );
// }


// // =========================================================
// // Generic API request
// // =========================================================

// async function apiRequest(
//   endpoint
// ) {

//   const response =
//     await fetch(
//       `${API_BASE_URL}${endpoint}`
//     );


//   if (!response.ok) {

//     let detail =
//       `API request failed with status ${response.status}.`;


//     try {

//       const errorBody =
//         await response.json();


//       if (errorBody.detail) {

//         detail =
//           errorBody.detail;
//       }

//     } catch {

//       // Keep default error message.

//     }


//     throw new Error(
//       detail
//     );
//   }


//   return response.json();
// }


// // =========================================================
// // RE2 — Network Summary
// // =========================================================

// export async function getNetworkSummary() {

//   return apiRequest(
//     "/network/summary"
//   );
// }


// // =========================================================
// // RE3 — Grid Activity
// // =========================================================

// export async function getGridActivity(
//   gridId
// ) {

//   const response =
//     await fetch(
//       `${API_BASE_URL}/network/grid/${gridId}`
//     );


//   if (!response.ok) {

//     if (response.status === 404) {

//       throw new Error(
//         `Grid ${gridId} was not found.`
//       );
//     }


//     throw new Error(
//       `Unable to retrieve activity for grid ${gridId}.`
//     );
//   }


//   return response.json();
// }


// // =========================================================
// // RE4 — Hotspots
// // =========================================================

// export async function getHotspots(
//   limit = 10,
//   asOf = null
// ) {

//   const params =
//     new URLSearchParams();


//   params.set(
//     "limit",
//     String(limit)
//   );


//   if (asOf) {

//     params.set(
//       "as_of",
//       asOf
//     );
//   }


//   return apiRequest(
//     `/network/hotspots?${params.toString()}`
//   );
// }


// // =========================================================
// // RE4 — Alerts
// // =========================================================

// export async function getAlerts(
//   limit = 10,
//   severity = null,
//   asOf = null
// ) {

//   const params =
//     new URLSearchParams();


//   params.set(
//     "limit",
//     String(limit)
//   );


//   if (severity) {

//     params.set(
//       "severity",
//       severity
//     );
//   }


//   if (asOf) {

//     params.set(
//       "as_of",
//       asOf
//     );
//   }


//   return apiRequest(
//     `/network/alerts?${params.toString()}`
//   );
// }


// =========================================================
// Phase 5 — API Client
// File: phase5/src/api/client.js
// =========================================================

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL;


// =========================================================
// Environment validation
// =========================================================

if (!API_BASE_URL) {

  throw new Error(
    "VITE_API_BASE_URL is not configured."
  );
}


// =========================================================
// Generic API request
// =========================================================

async function apiRequest(
  endpoint,
  options = {}
) {

  const response =
    await fetch(
      `${API_BASE_URL}${endpoint}`,
      {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {}),
        },
      }
    );


  if (!response.ok) {

    let detail =
      `API request failed with status ${response.status}.`;


    try {

      const errorBody =
        await response.json();


      if (errorBody.detail) {

        detail =
          errorBody.detail;
      }

    } catch {

      // Keep default error message.

    }


    throw new Error(
      detail
    );
  }


  return response.json();
}


// =========================================================
// RE2 — Network Summary
// =========================================================

export async function getNetworkSummary() {

  return apiRequest(
    "/network/summary"
  );
}


// =========================================================
// RE3 — Grid Activity
// =========================================================

export async function getGridActivity(
  gridId
) {

  const response =
    await fetch(
      `${API_BASE_URL}/network/grid/${gridId}`
    );


  if (!response.ok) {

    if (response.status === 404) {

      throw new Error(
        `Grid ${gridId} was not found.`
      );
    }


    throw new Error(
      `Unable to retrieve activity for grid ${gridId}.`
    );
  }


  return response.json();
}


// =========================================================
// RE4 — Hotspots
// =========================================================

export async function getHotspots(
  limit = 10,
  asOf = null
) {

  const params =
    new URLSearchParams();


  params.set(
    "limit",
    String(limit)
  );


  if (asOf) {

    params.set(
      "as_of",
      asOf
    );
  }


  return apiRequest(
    `/network/hotspots?${params.toString()}`
  );
}


// =========================================================
// RE4 — Alerts
// =========================================================

export async function getAlerts(
  limit = 10,
  severity = null,
  asOf = null
) {

  const params =
    new URLSearchParams();


  params.set(
    "limit",
    String(limit)
  );


  if (severity) {

    params.set(
      "severity",
      severity
    );
  }


  if (asOf) {

    params.set(
      "as_of",
      asOf
    );
  }


  return apiRequest(
    `/network/alerts?${params.toString()}`
  );
}


// =========================================================
// RE5 — Predictive Risk
// =========================================================

export async function predictRisk(
  predictionData
) {

  return apiRequest(
    "/network/predict-risk",
    {
      method: "POST",

      body: JSON.stringify(
        predictionData
      ),
    }
  );
}