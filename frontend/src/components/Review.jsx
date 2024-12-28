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

function Review({ date_stayed, title, text, overall, value, location, cleanliness, service, sleep_quality, rooms }) {
    return (
        <div className="bg-white shadow rounded-lg p-4">
            <h3 className="font-semibold text-lg mb-2">{title}</h3>
            <div className="flex justify-between items-center mb-2">
                <span className="text-sm text-gray-600">{date_stayed}</span>
            </div>
            <p className="text-gray-700 mb-4">{text}</p>
            <div className="grid grid-cols-2 gap-2">
                <RatingCategory name="Overall" value={overall || 0} />
                <RatingCategory name="Value" value={value || 0} />
                <RatingCategory name="Location" value={location || 0} />
                <RatingCategory name="Cleanliness" value={cleanliness || 0} />
                <RatingCategory name="Service" value={service || 0} />
                <RatingCategory name="Sleep Quality" value={sleep_quality || 0} />
                <RatingCategory name="Rooms" value={rooms || 0} />
            </div>
        </div>
    );
}

export default Review;