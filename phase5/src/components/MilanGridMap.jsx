// =========================================================
// RE4 — Milan Grid Map
// File: phase5/src/components/MilanGridMap.jsx
// =========================================================

import {
  MapContainer,
  TileLayer,
  GeoJSON,
  useMap,
} from "react-leaflet";

import {
  useEffect,
  useMemo,
} from "react";

import {
  useNavigate,
} from "react-router-dom";

import "leaflet/dist/leaflet.css";


// =========================================================
// Map configuration
// =========================================================

const MILAN_CENTER = [
  45.4642,
  9.1900,
];


// =========================================================
// Keep map viewport stable
// =========================================================

function MapBounds() {

  useMap();

  return null;
}


// =========================================================
// Milan Grid Map
// =========================================================

function MilanGridMap({
  geoJson,
  hotspots,
  alerts,
}) {

  const navigate =
    useNavigate();


  // =======================================================
  // Build operational status by grid
  // =======================================================

  const gridStatus =
    useMemo(() => {

      const statusMap = {};


      // ---------------------------------------------------
      // Hotspots
      // ---------------------------------------------------

      for (
        const hotspot of hotspots
      ) {

        const gridId =
          Number(
            hotspot.grid_id
          );


        statusMap[gridId] = {

          status:
            "ATTENTION",

          total_activity:
            Number(
              hotspot.total_activity
            ),

        };
      }


      // ---------------------------------------------------
      // Alerts override hotspot status
      // ---------------------------------------------------

      for (
        const alert of alerts
      ) {

        const gridId =
          Number(
            alert.grid_id
          );


        const severity =
          String(
            alert.severity || ""
          ).toUpperCase();


        let status =
          "ATTENTION";


        if (
          severity === "HIGH"
        ) {

          status =
            "HIGH";
        }


        statusMap[gridId] = {

          ...statusMap[gridId],

          status,

          severity,

          alert_type:
            alert.alert_type,

          current_activity:
            Number(
              alert.current_activity
            ),

        };
      }


      return statusMap;

    }, [
      hotspots,
      alerts,
    ]);


  // =======================================================
  // Render only operational polygons
  // =======================================================

  const visibleGeoJson =
    useMemo(() => {

      if (
        !geoJson ||
        !Array.isArray(
          geoJson.features
        )
      ) {

        return null;
      }


      const activeGridIds =
        new Set(
          Object.keys(
            gridStatus
          ).map(Number)
        );


      return {

        type:
          "FeatureCollection",

        features:
          geoJson.features.filter(
            (feature) => {

              const cellId =
                Number(
                  feature?.properties?.cellId
                );


              return activeGridIds.has(
                cellId
              );
            }
          ),

      };

    }, [
      geoJson,
      gridStatus,
    ]);


  // =======================================================
  // Polygon style
  // =======================================================

  function getStyle(
    feature
  ) {

    const gridId =
      Number(
        feature?.properties?.cellId
      );


    const information =
      gridStatus[gridId];


    // -----------------------------------------------------
    // NORMAL
    // -----------------------------------------------------

    if (
      !information
    ) {

      return {

        weight:
          1,

        fillOpacity:
          0.15,

        dashArray:
          undefined,

      };
    }


    // -----------------------------------------------------
    // HIGH
    // -----------------------------------------------------

    if (
      information.status === "HIGH"
    ) {

      return {

        weight:
          4,

        fillOpacity:
          0.70,

        dashArray:
          "8 4",

      };
    }


    // -----------------------------------------------------
    // ATTENTION
    // -----------------------------------------------------

    if (
      information.status === "ATTENTION"
    ) {

      return {

        weight:
          3,

        fillOpacity:
          0.55,

        dashArray:
          "4 4",

      };
    }


    // -----------------------------------------------------
    // Fallback NORMAL
    // -----------------------------------------------------

    return {

      weight:
        1,

      fillOpacity:
        0.25,

    };
  }


  // =======================================================
  // Polygon interactions
  // =======================================================

  function onEachFeature(
    feature,
    layer
  ) {

    const gridId =
      Number(
        feature?.properties?.cellId
      );


    const information =
      gridStatus[gridId];


    const status =
      information?.status ||
      "NORMAL";


    layer.bindTooltip(
      `Grid ${gridId} — ${status}`,
      {
        sticky: true,
      }
    );


    layer.on(
      "click",
      () => {

        navigate(
          `/grid/${gridId}`
        );

      }
    );

  }


  // =======================================================
  // Render
  // =======================================================

  return (

    <div className="milan-map">

      <MapContainer
        center={MILAN_CENTER}
        zoom={11}
        scrollWheelZoom={true}
        style={{
          height: "600px",
          width: "100%",
        }}
      >

        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />


        <MapBounds />


        {visibleGeoJson && (

          <GeoJSON
            key={JSON.stringify(
              {
                grids:
                  Object.keys(
                    gridStatus
                  ).sort(),

                statuses:
                  Object.fromEntries(
                    Object.entries(
                      gridStatus
                    ).map(
                      ([gridId, info]) => [
                        gridId,
                        info.status,
                      ]
                    )
                  ),
              }
            )}

            data={
              visibleGeoJson
            }

            style={
              getStyle
            }

            onEachFeature={
              onEachFeature
            }

          />

        )}

      </MapContainer>

    </div>
  );
}


export default MilanGridMap;