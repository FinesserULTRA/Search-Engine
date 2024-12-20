import { MapPin, Star } from "lucide-react";

const HotelCard = ({ id, name, image, rating, reviews, address }) => {

    const handleClick = () => {
        // Do something
    };

    return (
        <div className="rounded-lg border bg-card text-card-foreground shadow-sm" onClick={handleClick()} >
            <div className="relative overflow-hidden rounded-t-lg">
                <img className="object-cover" src={image} alt="Hotel" />
            </div>
            <div className="p-4">
                <h3 className="font-semibold text-md mb-2">{name}</h3>
                <div className="flex items-center gap-1 mb-9">
                    {[...Array(5)].map((_, i) => (
                        <Star
                            key={i}
                            className={`h-3 w-3 ${i < rating
                                ? "fill-primary text-primary"
                                : "fill-muted text-muted-foreground"
                                }`}
                        />
                    ))}
                    <span className="text-xs text-muted-foreground ml-0">
                        {reviews} reviews
                    </span>
                </div>
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <MapPin className="h-4 w-4 text-secondaryclr" />
                    <span>{address}</span>
                </div>
            </div>
        </div>
    );
};

export default HotelCard;
