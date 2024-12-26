import { Routes, Route, Navigate } from "react-router-dom";
import HomePage from "./pages/HomePage";
import SearchResultsPage from "./pages/SearchResultsPage";
import HotelDetailsPage from "./pages/HotelDetailsPage";
import DataFormPage from "./pages/DataFormPage";
import "./App.css";

function App() {
  return (
    <>
      <Routes>
        <Route path="/" element={<Navigate to="/home" />} />
        <Route path="/home" element={<HomePage />} />
        <Route path="/search" element={<SearchResultsPage />} />
        <Route path="/add" element={<DataFormPage />} />
        <Route path="/hotel/:id" element={<HotelDetailsPage />} />
        <Route path="*" element={<Navigate to="/home" />} />
      </Routes>
    </>
  );
}

export default App;
