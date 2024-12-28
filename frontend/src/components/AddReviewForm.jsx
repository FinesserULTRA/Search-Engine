import { useState } from 'react';
import { Star } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { useNavigate } from 'react-router-dom';

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

function AddReviewForm({ hotel_id }) {

    const id = hotel_id;
    const navigate = useNavigate();

    const [formData, setFormData] = useState({
        title: '',
        text: '',
        hotel_id: id,
        service: 0,
        cleanliness: 0,
        overall: 0,
        value: 0,
        location: 0,
        sleep_quality: 0,
        rooms: 0,
    });

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData((prev) => ({ ...prev, [name]: value }));
    };

    const handleRatingChange = (name, value) => {
        setFormData((prev) => ({ ...prev, [name]: value }));
    };

    const handleSubmit = (e) => {
        e.preventDefault();

        fetch(`http://127.0.0.1:8000/reviews`, {
            method: 'POST', // Specify the method
            headers: {
                'Content-Type': 'application/json', // Specify content type as JSON
            },
            body: JSON.stringify(formData),
        })
            .then((res) => res.json())
            .then((data) => {
                console.log('Review created:', data);
            })
            .catch((err) => {
                console.error('Error creating review:', err);
            });

        // Reset form after submission
        setFormData({
            title: '',
            text: '',
            hotel_id: 0,
            service: 0,
            cleanliness: 0,
            overall: 0,
            value: 0,
            location: 0,
            sleep_quality: 0,
            rooms: 0,
        });
        window.location.reload();
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
                <Label htmlFor="text">Text</Label>
                <Textarea
                    id="text"
                    name="text"
                    value={formData.text}
                    onChange={handleInputChange}
                    required
                />
            </div>
            <div className="grid grid-cols-2 gap-4">
                <RatingInput
                    name="overall"
                    value={formData.overall}
                    onChange={(value) => handleRatingChange('overall', value)}
                />
                <RatingInput
                    name="value"
                    value={formData.value}
                    onChange={(value) => handleRatingChange('value', value)}
                />
                <RatingInput
                    name="location"
                    value={formData.location}
                    onChange={(value) => handleRatingChange('location', value)}

                />
                <RatingInput
                    name="cleanliness"
                    value={formData.cleanliness}
                    onChange={(value) => handleRatingChange('cleanliness', value)}
                />
                <RatingInput
                    name="service"
                    value={formData.service}
                    onChange={(value) => handleRatingChange('service', value)}
                />
                <RatingInput
                    name="sleep_quality"
                    value={formData.sleep_quality}
                    onChange={(value) => handleRatingChange('sleep_quality', value)}
                />
                <RatingInput
                    name="rooms"
                    value={formData.rooms}
                    onChange={(value) => handleRatingChange('rooms', value)}
                />
            </div>
            <Button type="submit">Submit Review</Button>
        </form>
    );
}

export default AddReviewForm;