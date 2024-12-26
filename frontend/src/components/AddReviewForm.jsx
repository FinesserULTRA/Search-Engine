import { useState } from 'react';
import { Star } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";

const RatingInput = ({ name, value, onChange }) => (
    <div className="flex flex-col items-start">
        <Label htmlFor={name} className="mb-1">{name}</Label>
        <div className="flex">
            {[1, 2, 3, 4, 5].map((star) => (
                <button
                    key={star}
                    type="button"
                    onClick={() => onChange(star)}
                    className="focus:outline-none"
                >
                    <Star
                        className={`h-6 w-6 ${star <= value ? 'text-yellow-400 fill-yellow-400' : 'text-gray-300'
                            }`}
                    />
                </button>
            ))}
        </div>
    </div>
);

function AddReviewForm() {
    const [formData, setFormData] = useState({
        title: '',
        text: '',
        author: '',
        ratings: {
            overall: 0,
            value: 0,
            location: 0,
            cleanliness: 0,
            service: 0,
            sleep_quality: 0,
            rooms: 0,
        },
    });

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData((prev) => ({ ...prev, [name]: value }));
    };

    const handleRatingChange = (name, value) => {
        setFormData((prev) => ({
            ...prev,
            ratings: { ...prev.ratings, [name]: value },
        }));
    };

    const handleSubmit = (e) => {
        e.preventDefault();
        // Here you would typically send the formData to your backend
        console.log('Submitted review:', formData);
        // Reset form after submission
        setFormData({
            title: '',
            text: '',
            author: '',
            ratings: {
                overall: 0,
                value: 0,
                location: 0,
                cleanliness: 0,
                service: 0,
                sleep_quality: 0,
                rooms: 0,
            },
        });
    };

    return (
        <form onSubmit={handleSubmit} className="space-y-4">
            <div>
                <Label htmlFor="title">Title</Label>
                <Input
                    id="title"
                    name="title"
                    value={formData.title}
                    onChange={handleInputChange}
                    required
                />
            </div>
            <div>
                <Label htmlFor="text">Review</Label>
                <Textarea
                    id="text"
                    name="text"
                    value={formData.text}
                    onChange={handleInputChange}
                    required
                />
            </div>
            <div>
                <Label htmlFor="author">Your Name</Label>
                <Input
                    id="author"
                    name="author"
                    value={formData.author}
                    onChange={handleInputChange}
                    required
                />
            </div>
            <div className="grid grid-cols-2 gap-4">
                <RatingInput
                    name="Overall"
                    value={formData.ratings.overall}
                    onChange={(value) => handleRatingChange('overall', value)}
                />
                <RatingInput
                    name="Value"
                    value={formData.ratings.value}
                    onChange={(value) => handleRatingChange('value', value)}
                />
                <RatingInput
                    name="Location"
                    value={formData.ratings.location}
                    onChange={(value) => handleRatingChange('location', value)}
                />
                <RatingInput
                    name="Cleanliness"
                    value={formData.ratings.cleanliness}
                    onChange={(value) => handleRatingChange('cleanliness', value)}
                />
                <RatingInput
                    name="Service"
                    value={formData.ratings.service}
                    onChange={(value) => handleRatingChange('service', value)}
                />
                <RatingInput
                    name="Sleep Quality"
                    value={formData.ratings.sleep_quality}
                    onChange={(value) => handleRatingChange('sleep_quality', value)}
                />
                <RatingInput
                    name="Rooms"
                    value={formData.ratings.rooms}
                    onChange={(value) => handleRatingChange('rooms', value)}
                />
            </div>
            <Button type="submit">Submit Review</Button>
        </form>
    );
}

export default AddReviewForm;