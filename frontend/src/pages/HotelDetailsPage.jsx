import { useState } from "react";
import { ChevronLeft, ChevronRight, X } from 'lucide-react';
import Rating from "@/components/Rating";
import Review from "@/components/Review";
import AddReviewForm from "@/components/AddReviewForm";
import { Button } from "@/components/ui/button";
import { data, useParams } from "react-router-dom";
import { useEffect } from "react";

const images = [
    "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/09/35/09/e6/casablanca-hotel-times.jpg?w=1200&h=-1&s=1",
    "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/14/b7/ae/52/casablanca-hotel-by-library.jpg?w=1200&h=-1&s=1",
    "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/14/b7/ad/9e/blue-parrot-courtyard.jpg?w=1200&h=-1&s=1",
    "https://dynamic-media-cdn.tripadvisor.com/media/photo-o/14/b7/ae/31/marble-bathroom.jpg?w=1200&h=-1&s=1",
]

function HotelDetailsPage() {
    const [currentImageIndex, setCurrentImageIndex] = useState(0);
    const [showAllReviews, setShowAllReviews] = useState(false);
    const [showAddReview, setShowAddReview] = useState(false);
    const [hotelDetails, setHotelDetails] = useState({});
    const [reviews, setReviews] = useState([]);
    const { id } = useParams();

    useEffect(() => {
        // Fetch hotel data based on the ID
        fetch(`http://127.0.0.1:8000/hotels/${id}`)
            .then((res) => res.json())
            .then((data) => {
                setHotelDetails(data);
                setReviews(data.reviews);
            })
            .catch((err) => {
                console.log(err);
            });
    }, [id]);

    const nextImage = () => {
        setCurrentImageIndex(
            (prevIndex) => (prevIndex + 1) % images.length
        );
    };

    const prevImage = () => {
        setCurrentImageIndex(
            (prevIndex) =>
                (prevIndex - 1 + images.length) % images.length
        );
    };

    return (
        <div className="min-h-screen w-screen bg-primaryclr text-secondaryclr p-4 sm:p-8 relative">
            <div className="max-w-full mx-auto">
                <h1 className="text-2xl sm:text-3xl font-bold mb-2">{hotelDetails.name}</h1>
                <div className="flex flex-col sm:flex-row sm:items-center mb-4">
                    <p className="text-base sm:text-lg">{hotelDetails["street-address"] || "Unknown Address"}, {hotelDetails.locality || "Unknown Locality"}, {hotelDetails.region || "Unknown Region"}</p>
                    <span className="sm:ml-4 text-sm bg-blue-500 text-white px-2 py-1 rounded">
                        {hotelDetails.hotel_class} Star Hotel
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
                                src={images[currentImageIndex]}
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
                    <Rating rating={hotelDetails.overall} />
                    <span className="ml-2 text-base sm:text-lg font-semibold">
                        {hotelDetails.overall ? hotelDetails.overall.toFixed(1) : "N/A"}
                    </span>
                </div>

                <h2 className="text-xl sm:text-2xl font-semibold mb-4">Reviews</h2>
                <div className="space-y-6">
                    {reviews.slice(0, 3).map((review, index) => (
                        <Review
                            key={index}
                            {...review}
                        />
                    ))}
                </div>
                <div className="mt-4 space-x-4">
                    {reviews.length > 3 && (
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
                            {reviews.map((review, index) => (
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
                        <AddReviewForm hotel_id={id} />
                    </div>
                </div>
            )}
        </div>
    );
}

export default HotelDetailsPage;