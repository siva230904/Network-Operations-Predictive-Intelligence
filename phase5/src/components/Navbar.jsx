// // =========================================================
// // Phase 5 — Navigation
// // File: phase5/src/components/Navbar.jsx
// // =========================================================

// import {
//   NavLink,
//   Link,
// } from "react-router-dom";


// // =========================================================
// // Navbar
// // =========================================================

// function Navbar() {

//   return (

//     <nav className="navbar">

//       {/* =================================================
//           Brand
//           ================================================= */}

//       <div className="navbar-brand">

//         <Link to="/">
//           Network Operations
//         </Link>

//       </div>


//       {/* =================================================
//           Navigation Links
//           ================================================= */}

//       <div className="navbar-links">

//         <NavLink
//           to="/"
//           end
//           className={({ isActive }) =>
//             isActive
//               ? "nav-link active"
//               : "nav-link"
//           }
//         >
//           Overview
//         </NavLink>


//         <NavLink
//           to="/grid"
//           className={({ isActive }) =>
//             isActive
//               ? "nav-link active"
//               : "nav-link"
//           }
//         >
//           Grid Explorer
//         </NavLink>


//         <NavLink
//           to="/hotspots"
//           className={({ isActive }) =>
//             isActive
//               ? "nav-link active"
//               : "nav-link"
//           }
//         >
//           Hotspots & Alerts
//         </NavLink>

//       </div>

//     </nav>
//   );
// }


// export default Navbar;


// =========================================================
// Phase 5 — Navigation
// File: phase5/src/components/Navbar.jsx
// =========================================================

import {
  NavLink,
  Link,
} from "react-router-dom";


// =========================================================
// Navbar
// =========================================================

function Navbar() {

  return (

    <nav className="navbar">

      {/* =================================================
          Brand
          ================================================= */}

      <div className="navbar-brand">

        <Link to="/">
          Network Operations
        </Link>

      </div>


      {/* =================================================
          Navigation Links
          ================================================= */}

      <div className="navbar-links">

        <NavLink
          to="/"
          end
          className={({ isActive }) =>
            isActive
              ? "nav-link active"
              : "nav-link"
          }
        >
          Overview
        </NavLink>


        <NavLink
          to="/grid"
          className={({ isActive }) =>
            isActive
              ? "nav-link active"
              : "nav-link"
          }
        >
          Grid Explorer
        </NavLink>


        <NavLink
          to="/hotspots"
          className={({ isActive }) =>
            isActive
              ? "nav-link active"
              : "nav-link"
          }
        >
          Hotspots & Alerts
        </NavLink>


        <NavLink
          to="/predict-risk"
          className={({ isActive }) =>
            isActive
              ? "nav-link active"
              : "nav-link"
          }
        >
          Predictive Risk
        </NavLink>

      </div>

    </nav>
  );
}


export default Navbar;