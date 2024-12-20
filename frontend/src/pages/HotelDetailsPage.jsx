import { useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import Rating from "@/components/Rating";
import Review from "@/components/Review";
import { Button } from "@/components/ui/button";

const hotelData = {
    name: "Casablanca Hotel By Library Hotel Collection",
    address: "147 West 43rd Street, New York City, NY",
    rating: 4.5,
    reviews: [
        {
            author: "John Doe",
            date: "May 15, 2023",
            rating: 5,
            comment: "Excellent hotel with great service!",
        },
        {
            author: "Jane Smith",
            date: "April 30, 2023",
            rating: 4,
            comment: "Very good location, clean rooms.",
        },
        {
            author: "Mike Johnson",
            date: "April 22, 2023",
            rating: 5,
            comment: "Fantastic experience, will definitely come back!",
        },
    ],
    images: [
        "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/09/35/09/e6/casablanca-hotel-times.jpg?w=1200&h=-1&s=1",
        "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/14/b7/ae/52/casablanca-hotel-by-library.jpg?w=1200&h=-1&s=1",
        "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/14/b7/ad/9e/blue-parrot-courtyard.jpg?w=1200&h=-1&s=1",
        "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/14/b7/ae/31/marble-bathroom.jpg?w=1200&h=-1&s=1",
    ],
    description:
        "Experience the charm of Casablanca Hotel, nestled in the heart of New York City. Our boutique hotel offers a perfect blend of comfort and luxury, with easy access to Times Square and Broadway theaters. Enjoy our complimentary continental breakfast and evening wine and cheese receptions.",
};

function HotelDetailsPage() {
    const [currentImageIndex, setCurrentImageIndex] = useState(0);

    const nextImage = () => {
        setCurrentImageIndex(
            (prevIndex) => (prevIndex + 1) % hotelData.images.length
        );
    };

    const prevImage = () => {
        setCurrentImageIndex(
            (prevIndex) =>
                (prevIndex - 1 + hotelData.images.length) % hotelData.images.length
        );
    };

    return (
        <div className="min-h-screen bg-primaryclr text-secondaryclr p-8">
            <div className="max-w-4xl mx-auto">
                <h1 className="text-3xl font-bold mb-4">{hotelData.name}</h1>
                <p className="text-lg mb-4">{hotelData.address}</p>

                <div className="relative aspect-video mb-8">
                    <img src={hotelData.images[currentImageIndex]} alt="Hotel" className="object-cover rounded-lg" />
                    <Button
                        variant="outline"
                        size="icon"
                        className="absolute top-1/2 left-4 transform -translate-y-1/2 bg-primaryclr/80"
                        onClick={prevImage}
                    >
                        <ChevronLeft className="h-6 w-6" />
                    </Button>
                    <Button
                        variant="outline"
                        size="icon"
                        className="absolute top-1/2 right-4 transform -translate-y-1/2 bg-primaryclr/80"
                        onClick={nextImage}
                    >
                        <ChevronRight className="h-6 w-6" />
                    </Button>
                </div>

                <div className="flex items-center mb-4">
                    <Rating rating={hotelData.rating} />
                    <span className="ml-2 text-lg font-semibold">
                        {hotelData.rating.toFixed(1)}
                    </span>
                </div>

                <p className="text-lg mb-8">{hotelData.description}</p>

                <h2 className="text-2xl font-semibold mb-4">Reviews</h2>
                <div className="space-y-4">
                    {hotelData.reviews.map((review, index) => (
                        <Review key={index} {...review} />
                    ))}
                </div>
            </div>
        </div>
    );
}

export default HotelDetailsPage;
