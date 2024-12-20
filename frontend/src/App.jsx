import { Routes, Route, Navigate } from "react-router-dom";
import HomePage from "./pages/HomePage";
import "./App.css";
import SearchResultsPage from "./pages/SearchResultsPage";

function App() {
  return (
    <>
      <Routes>
        <Route path="/" element={<Navigate to="/home" />} />
        <Route path="/home" element={<HomePage />} />
        <Route path="/search" element={<SearchResultsPage />} />
        <Route path="*" element={<Navigate to="/home" />} />
      </Routes>
    </>
  );
}

export default App;
