import SearchBar from "@/components/SearchBar"
import HotelCard from "@/components/HotelCard"
import { Button } from "@/components/ui/button"

const hotels = Array(10).fill({
    name: "Casablanca Hotel By Library Hotel Collection",
    image: "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/09/35/09/e6/casablanca-hotel-times.jpg?w=2000&h=-1&s=1",
    rating: 5,
    reviews: 1234,
    address: "147 West 43rd Street, New York City, NY"
})

const SearchResultsPage = () => {
    return (
        <div>
            <div className="container mx-auto py-12 px-4 space-y-8 w-max">
                <div className="flex justify-center w-full px-4">
                    <div className="w-full max-w-4xl">
                        <SearchBar />
                    </div>
                </div>
                <div className="flex items-center justify-between max-w-[1400px] mx-auto w-full px-4">
                    <h1 className="text-2xl font-semibold text-gray-900">Hotels</h1>
                    <Button variant="outline" className="px-6">Sort</Button>
                </div>
                <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-6 max-w-[1400px] mx-auto px-4">
                    {hotels.map((hotel, i) => (
                        <HotelCard key={i} {...hotel} />
                    ))}
                </div>
            </div>
        </div>
    )
}

export default SearchResultsPage;