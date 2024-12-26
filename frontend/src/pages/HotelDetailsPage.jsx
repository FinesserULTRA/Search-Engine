import { useState } from "react";
import { ChevronLeft, ChevronRight, X } from 'lucide-react';
import Rating from "@/components/Rating";
import Review from "@/components/Review";
import AddReviewForm from "@/components/AddReviewForm";
import { Button } from "@/components/ui/button";

const hotelData = {
    name: "Casablanca Hotel By Library Hotel Collection",
    region: "New York",
    street_address: "147 West 43rd Street",
    locality: "New York City",
    hotel_class: 4,
    rating: 4.5,
    reviews: [
        {
            author: "John Doe",
            date: "May 15, 2023",
            title: "Excellent Stay in the Heart of NYC",
            text: "Excellent hotel with great service! The location is perfect for exploring Times Square and Broadway.",
            ratings: {
                overall: 5,
                value: 4,
                location: 5,
                cleanliness: 5,
                service: 5,
                sleep_quality: 4,
                rooms: 4
            }
        },
        {
            author: "Jane Smith",
            date: "April 30, 2023",
            title: "Great Location, Clean Rooms",
            text: "Very good location, clean rooms. The staff was friendly and helpful throughout our stay.",
            ratings: {
                overall: 4,
                value: 4,
                location: 5,
                cleanliness: 5,
                service: 4,
                sleep_quality: 3,
                rooms: 4
            }
        },
        {
            author: "Mike Johnson",
            date: "April 22, 2023",
            title: "Fantastic Experience, Will Return!",
            text: "Fantastic experience, will definitely come back! The complimentary breakfast and evening reception were great touches.",
            ratings: {
                overall: 5,
                value: 5,
                location: 5,
                cleanliness: 5,
                service: 5,
                sleep_quality: 4,
                rooms: 5
            }
        },
        {
            author: "Mike Johnson",
            date: "April 22, 2023",
            title: "Fantastic Experience, Will Return!",
            text: "Fantastic experience, will definitely come back! The complimentary breakfast and evening reception were great touches.",
            ratings: {
                overall: 5,
                value: 5,
                location: 5,
                cleanliness: 5,
                service: 5,
                sleep_quality: 4,
                rooms: 5
            }
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
    const [showAllReviews, setShowAllReviews] = useState(false);
    const [showAddReview, setShowAddReview] = useState(false);

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
        <div className="min-h-screen min-w-full bg-primaryclr text-secondaryclr p-4 sm:p-8 relative">
            <div className="max-w-full mx-auto">
                <h1 className="text-2xl sm:text-3xl font-bold mb-2">{hotelData.name}</h1>
                <div className="flex flex-col sm:flex-row sm:items-center mb-4">
                    <p className="text-base sm:text-lg">{hotelData.street_address}, {hotelData.locality}, {hotelData.region}</p>
                    <span className="sm:ml-4 text-sm bg-blue-500 text-white px-2 py-1 rounded">
                        {hotelData.hotel_class} Star Hotel
                    </span>
                </div>

                <div className="relative w-full max-w-md mx-auto mb-6 sm:mb-8 flex items-center">
                    <Button
                        variant="outline"
                        size="icon"
                        className="mr-2 w-10 h-10 rounded-full flex items-center justify-center"
                        onClick={prevImage}
                    >
                        <ChevronLeft className="h-6 w-6" />
                    </Button>
                    <div className="flex-grow">
                        <div className="aspect-[4/3]">
                            <img
                                src={hotelData.images[currentImageIndex]}
                                alt="Hotel"
                                className="object-cover w-full h-full rounded-lg"
                            />
                        </div>
                    </div>
                    <Button
                        variant="outline"
                        size="icon"
                        className="ml-2 w-10 h-10 rounded-full flex items-center justify-center"
                        onClick={nextImage}
                    >
                        <ChevronRight className="h-6 w-6" />
                    </Button>
                </div>

                <div className="flex items-center mb-4">
                    <Rating rating={hotelData.rating} />
                    <span className="ml-2 text-base sm:text-lg font-semibold">
                        {hotelData.rating.toFixed(1)}
                    </span>
                </div>

                <p className="text-base sm:text-lg mb-6 sm:mb-8">{hotelData.description}</p>

                <h2 className="text-xl sm:text-2xl font-semibold mb-4">Reviews</h2>
                <div className="space-y-6">
                    {hotelData.reviews.slice(0, 3).map((review, index) => (
                        <Review key={index} {...review} />
                    ))}
                </div>
                <div className="mt-4 space-x-4">
                    {hotelData.reviews.length > 3 && (
                        <Button
                            onClick={() => setShowAllReviews(true)}
                        >
                            Show more reviews
                        </Button>
                    )}
                    <Button
                        onClick={() => setShowAddReview(true)}
                        variant="outline"
                    >
                        Add a review
                    </Button>
                </div>
            </div>

            {showAllReviews && (
                <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
                    <div className="bg-white rounded-lg p-6 w-full max-w-3xl max-h-[90vh] overflow-y-auto relative">
                        <Button
                            variant="outline"
                            size="icon"
                            className="absolute top-2 right-2"
                            onClick={() => setShowAllReviews(false)}
                        >
                            <X className="h-4 w-4" />
                        </Button>
                        <h2 className="text-2xl font-semibold mb-4 text-gray-800">All Reviews</h2>
                        <div className="space-y-6">
                            {hotelData.reviews.map((review, index) => (
                                <Review key={index} {...review} />
                            ))}
                        </div>
                    </div>
                </div>
            )}

            {showAddReview && (
                <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
                    <div className="bg-white rounded-lg p-6 w-full max-w-3xl max-h-[90vh] overflow-y-auto relative">
                        <Button
                            variant="outline"
                            size="icon"
                            className="absolute top-2 right-2"
                            onClick={() => setShowAddReview(false)}
                        >
                            <X className="h-4 w-4" />
                        </Button>
                        <h2 className="text-2xl font-semibold mb-4 text-gray-800">Add a Review</h2>
                        <AddReviewForm />
                    </div>
                </div>
            )}
        </div>
    );
}

export default HotelDetailsPage;