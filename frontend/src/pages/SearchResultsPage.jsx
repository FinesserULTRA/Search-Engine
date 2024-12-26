import HotelCard from "@/components/HotelCard";
import SearchBar from "@/components/SearchBar";

const hotels = Array(10).fill({
  id: 1,
  name: "Casablanca Hotel By Library Hotel Collection",
  image:
    "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/09/35/09/e6/casablanca-hotel-times.jpg?w=2000&h=-1&s=1",
  rating: 5,
  reviews: 1234,
  address: "147 West 43rd Street, New York City, NY",
});

function SearchResultsPage() {
  return (
    <div className="min-h-screen w-full bg-primaryclr p-8">
      <div className="max-w-full mx-auto min-w-[320px] space-y-8">
        <div className="flex justify-center w-full px-4">
          <div className="w-full max-w-4xl">
            <SearchBar />
          </div>
        </div>
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold font-sans text-secondaryclr">Hotels</h1>  
        </div>

        <div className="grid font-sans grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
          {hotels.map((hotel, index) => (
            <HotelCard
              key={index}
              id={hotel.id}
              name={hotel.name}
              image={hotel.image}
              rating={hotel.rating}
              reviews={hotel.reviews}
              address={hotel.address}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

export default SearchResultsPage;