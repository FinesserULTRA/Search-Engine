import HotelCard from "@/components/HotelCard";
import SearchBar from "@/components/SearchBar";
import { useLocation } from "react-router-dom";

function SearchResultsPage() {
  const location = useLocation();
  const { data } = location.state;
  const hotels = data.results;
  const totalResults = data.total_matches;

  return (
    <div className="min-h-screen w-screen bg-primaryclr p-8">
      <div className="w-full mx-auto space-y-8">
        <div className="flex justify-center w-full px-4">
          <div className="w-full max-w-4xl">
            <SearchBar />
          </div>
        </div>
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold font-sans text-secondaryclr">Hotels</h1>
          <p className="text-sm font-sans text-secondaryclr">{totalResults} results</p>
        </div>

        <div className="grid font-sans grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
          {hotels.map((hotel, index) => (
            <HotelCard
              key={index}
              id={hotel.hotel_id}
              name={hotel.name}
              image={"https://dynamic-media-cdn.tripadvisor.com/media/photo-o/09/35/09/e6/casablanca-hotel-times.jpg?w=2000&h=-1&s=1"}
              rating={hotel.overall}
              reviews={hotel.review_count}
              address={hotel["street-address"]}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

export default SearchResultsPage;