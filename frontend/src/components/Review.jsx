import { Star } from 'lucide-react';

const RatingCategory = ({ name, value }) => (
    <div className="flex items-center justify-between">
        <span className="text-sm">{name}:</span>
        <div className="flex">
            {[1, 2, 3, 4, 5].map((star) => (
                <Star
                    key={star}
                    className={`h-4 w-4 ${star <= value ? 'text-yellow-400 fill-yellow-400' : 'text-gray-300'
                        }`}
                />
            ))}
        </div>
    </div>
);

function Review({ author, date, title, text, ratings }) {
    return (
        <div className="bg-white shadow rounded-lg p-4">
            <h3 className="font-semibold text-lg mb-2">{title}</h3>
            <div className="flex justify-between items-center mb-2">
                <span className="text-sm text-gray-600">{author}</span>
                <span className="text-sm text-gray-600">{date}</span>
            </div>
            <p className="text-gray-700 mb-4">{text}</p>
            <div className="grid grid-cols-2 gap-2">
                <RatingCategory name="Overall" value={ratings.overall} />
                <RatingCategory name="Value" value={ratings.value} />
                <RatingCategory name="Location" value={ratings.location} />
                <RatingCategory name="Cleanliness" value={ratings.cleanliness} />
                <RatingCategory name="Service" value={ratings.service} />
                <RatingCategory name="Sleep Quality" value={ratings.sleep_quality} />
                <RatingCategory name="Rooms" value={ratings.rooms} />
            </div>
        </div>
    );
}

export default Review;