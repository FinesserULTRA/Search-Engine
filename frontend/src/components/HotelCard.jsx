import { MapPin, Star } from 'lucide-react';
import { Link } from 'react-router-dom';
// import { useNavigate } from 'react-router-dom';

function HotelCard({id, name, image, rating, reviews, address}) {

    // const navigate = useNavigate()

    return (
        <Link to={`/hotel/${id}`} className="block">
            <div className="rounded-lg border bg-card text-card-foreground shadow-sm transition-all duration-300 hover:shadow-lg hover:-translate-y-1 cursor-pointer">
                <div className="relative overflow-hidden rounded-t-lg">
                    <img className="object-cover w-full h-48" src={image} alt={name} />
                </div>
                <div className="p-4">
                    <h3 className="font-semibold text-md mb-2 line-clamp-2">{name}</h3>
                    <div className="flex items-center gap-1 mb-2">
                        {[...Array(5)].map((_, i) => (
                            <Star
                                key={i}
                                className={`h-3 w-3 ${i < rating
                                    ? "fill-primary text-primary"
                                    : "fill-muted text-muted-foreground"
                                    }`}
                            />
                        ))}
                        <span className="text-xs text-muted-foreground ml-1">
                            {reviews} reviews
                        </span>
                    </div>
                    <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <MapPin className="h-4 w-4 text-secondaryclr" />
                        <span className="line-clamp-1">{address}</span>
                    </div>
                </div>
            </div>
        </Link>
    );
};

export default HotelCard;